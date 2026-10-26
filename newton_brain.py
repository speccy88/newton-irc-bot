#!/usr/bin/env python3
"""Newton's brain: a LangChain ReAct agent deciding every IRC interaction.

Architecture (LangChain agent pivot, 2026-10-26 — supersedes both the
cron-worker brain and the single-call urllib brain):
  newton_bot.py (persistent process) is the BODY: sockets, logging,
  interaction queue, human-delayed outbox sends, nick reclaim.
  This module is the BRAIN: a ReAct agent that, per queued interaction,
  reads thread context, reads Newton's recent lines, optionally searches
  the web, decides reply vs skip, writes the reply to the outbox, and
  closes the interaction with scores + an analyst note.

WHY ReAct (and not deepagents or a single call):
  - deepagents is built for long-horizon, multi-file work; an IRC reply
    is a 2-minute decision. Using it here would gold-plate and multiply
    model calls against a $1 key ceiling.
  - A single chat call is cheapest, but the operator asked for an agent that
    reasons with tools — and tools are what let Newton check his own
    history (no repeats), verify a claim before arguing it (search),
    and write his own scores/notes. ReAct with 5 tools and a tight
    iteration cap is the smallest framework that does all of that.
  - Fallback if agentic cost threatens the ceiling: the single-call
    chain (same prompt, one completion, no tools). It is documented,
    not built — the live test decides whether it is needed.

FRAMEWORK: langchain-classic ReAct (AgentExecutor) — chosen because
langchain 1.x's create_agent exposes no max_iterations bound, and the
$1 ceiling demands a hard cap on calls per interaction. Classic gives
max_iterations + max_execution_time + callback token tracking out of
the box. Venv: ~/workspace/newton-irc/venv/ (langchain, langchain-openai,
langchain-classic, ddgs). The bot runs under the venv python.

PERSONA: the agent's core prompt is soul/SOUL.md, loaded from disk on
every agent build (edits take effect on the next interaction, no
restart). SOUL.md is the American Psycho persona (dark comedy jerk,
gaslighter edge, loves arguing, provocative troll, havoc, hates Trump
and billionaires, 9 topics, human-imperfect style, thank-you rule) +
log-lessons as behavior rules + hard boundaries. There is NO USER.md
for Newton and nothing from any user file was copied into SOUL.md —
by privacy design, Newton can never know anything about his
owner/operator. His memory is the database, not a person-file.

AUTH: surrogate only, never the raw key. Per interaction, a fresh
surrogate is fetched via dynamic_credentials (authd Unix socket) and
passed as ChatOpenAI's api_key with base_url openrouter.ai/api/v1 and
an httpx client with redirects disabled — mirroring the verified
urllib helper's behavior. The surrogate is never logged, never stored,
never in code/env/DB/prompts. On surrogate failure the interaction
stays pending (fail closed, no fallback).

MODEL (cheap-but-good sweet spot, verified live from the OpenRouter
catalog at build time 2026-10-26):
  meta-llama/llama-3.3-70b-instruct — $0.10/M input, $0.32/M output.
  NOT the absolute cheapest (deepseek-v4.1-flash, $0.02/$1.00) and NOT
  flagship: a 70B model with strong tool-call adherence and reasoning,
  at essentially the same per-interaction cost as the cheapest flash
  (output tokens dominate, and llama's output is 3x cheaper than
  flash's). Estimated ~$0.0016 per agentic interaction (multi-call
  loop with growing history). $1 ceiling ≈ 600 interactions.
  If the live test shows weak replies, step up one tier within the
  cheap band (gemini-2.5-flash) and say so.

SPEND GUARDRAILS (hard, against the $1 key ceiling):
  - Cumulative spend cap $0.75 (75% of the key limit): stop calling,
    leave pending, log. Never exceed it.
  - Daily interaction cap 100 (~$0.16/day at estimate): stop till
    tomorrow, log.
  - max_iterations=8, max_execution_time=180s per interaction.
  - Max 2 web searches per interaction (free, but bounded).
  - Every interaction logged to brain_calls.jsonl: model, total
    prompt/completion tokens, cost USD, LLM calls, decision, searches.
  - On API failure/rate-limit: keep pending, back off, retry later.
    NEVER a template fallback — silence beats a stock line.

PRIVACY: prompts contain ONLY public IRC content + SOUL.md + rules.
A denylist check refuses any prompt containing owner-identifying
words (fail closed). Search queries are denylisted the same way.
"""
import json
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, "/opt/muse/skills/custom-connector-setup/bin")
import newton_db

BASE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE, "newton.db")
SOUL_PATH = os.path.join(BASE, "soul", "SOUL.md")
OUTBOX_PATH = os.path.join(BASE, "outbox.jsonl")
CALLS_LOG = os.path.join(BASE, "brain_calls.jsonl")
SEARCHES_LOG = os.path.join(BASE, "brain_searches.jsonl")

MODEL = "meta-llama/llama-3.3-70b-instruct"
API_BASE = "https://openrouter.ai/api/v1"
PRICE_IN_PER_M = 0.10
PRICE_OUT_PER_M = 0.32
MAX_ITERATIONS = 8
MAX_EXECUTION_TIME_S = 180
MAX_REPLY_CHARS = 400
MAX_SEARCHES_PER_INTERACTION = 2
CUMULATIVE_SPEND_CAP_USD = 0.75
DAILY_INTERACTION_CAP = 100
CONNECTOR = "custom.openrouter"
QUEBEC = ZoneInfo("America/Toronto")

# Owner-identifying words that must NEVER appear in a prompt or a search
# query (fail closed). The prompt refers to the owner only as
# "your owner/operator".
#
# The words themselves live OUTSIDE this repo, in operator_denylist.txt
# (one word per line, same directory as this file, gitignored — never
# commit it). That keeps the operator's identifiers out of a public
# checkout while keeping the guard live. If the file is missing or
# empty, the guard matches nothing and a warning is printed at import:
# CREATE THE FILE before running the brain — the guard is useless
# without it.
def _load_denylist():
    words = []
    try:
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "operator_denylist.txt")) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    words.append(line)
    except OSError:
        pass
    if not words:
        print("WARNING: operator_denylist.txt missing/empty — privacy "
              "denylist is inactive. Create it before running the brain.",
              file=sys.stderr)
        return re.compile(r"(?!x)x")
    return re.compile(r"\b(" + "|".join(re.escape(w) for w in words) + r")\b",
                       re.IGNORECASE)


_DENY = _load_denylist()


def load_soul():
    """Read SOUL.md (the agent's core persona prompt). Raises if the
    file is missing — no persona, no brain."""
    with open(SOUL_PATH) as f:
        text = f.read()
    if not text.strip():
        raise ValueError("SOUL.md is empty")
    if _DENY.search(text):
        raise ValueError("SOUL.md tripped the privacy denylist")
    return text


def fetch_surrogate():
    """Fetch a fresh auth surrogate for the OpenRouter connection.
    Returns the surrogate string. Raises on any failure (fail closed).
    The surrogate is NOT the raw key and is never logged or stored."""
    from dynamic_credentials import dynamic_credential_entry
    entry = dynamic_credential_entry(CONNECTOR)
    placement = entry.get("placement")
    if placement != "bearer_header":
        raise ValueError(f"unexpected credential placement: {placement!r}")
    surrogate = str(entry.get("surrogate", "")).strip()
    if not surrogate.startswith("hsurr:"):
        raise ValueError("authd returned a non-surrogate value")
    return surrogate


def _today_quebec():
    return datetime.now(QUEBEC).strftime("%Y-%m-%d")


def _read_jsonl(path):
    try:
        with open(path) as f:
            for line in f:
                try:
                    yield json.loads(line)
                except ValueError:
                    continue
    except OSError:
        return


def cumulative_spend():
    return sum(float(e.get("cost_usd") or 0) for e in _read_jsonl(CALLS_LOG))


def interactions_today():
    today = _today_quebec()
    return sum(1 for e in _read_jsonl(CALLS_LOG)
               if e.get("day") == today)


def log_interaction(interaction_id, server, channel, prompt_tokens,
                    completion_tokens, cost_usd, llm_calls, decision,
                    searches):
    entry = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "day": _today_quebec(),
        "interaction_id": interaction_id,
        "server": server,
        "channel": channel,
        "model": MODEL,
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "cost_usd": round(cost_usd, 6),
        "llm_calls": llm_calls,
        "decision": decision,
        "searches": searches,
    }
    with open(CALLS_LOG, "a") as f:
        f.write(json.dumps(entry) + "\n")


class TokenUsageHandler:
    """LangChain callback handler summing token usage across all LLM
    calls in one agent run (classic API: on_llm_end with token_usage)."""

    def __init__(self):
        self.prompt_tokens = 0
        self.completion_tokens = 0
        self.calls = 0

    def on_llm_end(self, response, **kwargs):
        try:
            usage = (response.llm_output or {}).get("token_usage") or {}
            self.prompt_tokens += int(usage.get("prompt_tokens") or 0)
            self.completion_tokens += int(usage.get("completion_tokens") or 0)
            self.calls += 1
        except Exception:
            pass


def _db():
    return sqlite3.connect(DB_PATH, timeout=30)


def _fmt_context(rows):
    lines = []
    for sender, direction, ts, text in rows:
        hhmm = (ts or "")[11:16]
        lines.append(f"[{hhmm}] {sender}: {text}")
    return "\n".join(lines) if lines else "(nothing yet)"


def build_tools(interaction):
    """Build the agent's 5 tools, closed over one interaction's state.
    Each tool opens its own DB connection (the agent runs in a worker
    thread; the bot's main connection must never be shared across
    threads)."""
    server = interaction["server"]
    channel = interaction["channel"]
    iid = interaction["id"]
    state = {"reply_written": False, "reply_text": None,
             "closed": False, "decision": None, "searches": []}

    def _tool(fn):
        from langchain_core.tools import tool as tool_decorator
        return tool_decorator(fn)

    @_tool
    def read_thread_context(server_arg: str, channel_arg: str) -> str:
        """Read the last 15 messages in a channel (oldest first). Call
        this FIRST for the channel in the trigger, before deciding."""
        conn = _db()
        try:
            rows = conn.execute(
                "SELECT sender, direction, ts_quebec, text FROM messages"
                " WHERE server = ? AND channel = ? ORDER BY id DESC LIMIT 15",
                (server_arg, channel_arg)).fetchall()
        finally:
            conn.close()
        return _fmt_context(list(reversed(rows)))

    @_tool
    def read_recent_outbound(server_arg: str, channel_arg: str) -> str:
        """Read Newton's last 20 lines in a channel (newest first).
        You MUST read this before writing a reply: never repeat or
        contradict anything here."""
        conn = _db()
        try:
            rows = conn.execute(
                "SELECT text FROM messages WHERE server = ? AND channel = ?"
                " AND direction = 'outbound' ORDER BY id DESC LIMIT 20",
                (server_arg, channel_arg)).fetchall()
        finally:
            conn.close()
        lines = [r[0] for r in rows]
        return "\n".join(f"- {t}" for t in lines) if lines \
            else "(nothing said here yet)"

    @_tool
    def web_search(query: str) -> str:
        """Search the public web for a factual or tech question (max 2
        searches per interaction). Use it to verify a claim before
        arguing it, or to answer a help question with facts instead of
        bluffing. NEVER search for anything about a person, a private
        individual, or this project. Returns short snippets."""
        if _DENY.search(query):
            return "REFUSED: that query is not allowed."
        if len(state["searches"]) >= MAX_SEARCHES_PER_INTERACTION:
            return "Search budget used (2 per interaction). Decide without it."
        try:
            from ddgs import DDGS
            # DDG's own endpoint is DNS-blocked from this VM; the bing
            # backend through the same ddgs package works (verified).
            results = list(DDGS().text(query, max_results=3, backend="bing"))
        except Exception as e:
            return f"Search unavailable ({type(e).__name__}). Decide without it."
        state["searches"].append(query)
        with open(SEARCHES_LOG, "a") as f:
            f.write(json.dumps({
                "ts_utc": datetime.now(timezone.utc).isoformat(),
                "interaction_id": iid, "server": server, "channel": channel,
                "query": query, "results": len(results)}) + "\n")
        if not results:
            return "No results. Decide without it."
        out = []
        for r in results:
            out.append(f"- {r.get('title', '')}: {r.get('body', '')}"[:300])
        return "\n".join(out)

    @_tool
    def write_outbox_reply(server_arg: str, channel_arg: str, text: str) -> str:
        """Queue a reply for Newton to send (the bot sends it with a
        human delay). Text must be 1-3 short lines, under 400 chars,
        fresh (never a repeat of Newton's recent lines in this channel).
        Call this ONLY when you decided to reply, and BEFORE
        close_interaction."""
        text = (text or "").strip()
        if not text:
            return "ERROR: empty reply, nothing queued."
        if len(text) > MAX_REPLY_CHARS:
            cut = text[:MAX_REPLY_CHARS].rsplit(" ", 1)[0] or text[:MAX_REPLY_CHARS]
            text = cut
        conn = _db()
        try:
            recent = [r[0] for r in conn.execute(
                "SELECT text FROM messages WHERE server = ? AND channel = ?"
                " AND direction = 'outbound' ORDER BY id DESC LIMIT 20",
                (server_arg, channel_arg)).fetchall()]
        finally:
            conn.close()
        if text in recent:
            return ("ERROR: that repeats a previous line of Newton's in "
                    "this channel. Write something fresh instead.")
        with open(OUTBOX_PATH, "a") as f:
            f.write(json.dumps({"server": server_arg, "channel": channel_arg,
                                "text": text}) + "\n")
        state["reply_written"] = True
        state["reply_text"] = text
        return "Queued for sending (bot delivers with a human delay)."

    @_tool
    def close_interaction(engagement: int, detection_risk: int,
                          reply_quality: int, heat: int, repeated: int,
                          note: str, skip_reason: str = "",
                          searches_used: str = "none") -> str:
        """CLOSE the interaction — you MUST call this exactly once, as
        your LAST step. Scores: engagement 0-100 (real human engaging
        Newton vs noise), detection_risk 0-100 (0 nobody suspected a
        bot, 100 explicit callout), reply_quality 0-100 or -1 when
        skipping (relevant? fresh? in voice? a reply ignoring the
        question scores under 30), heat 0-1000 (anger/swearing/kicks,
        same scale as chaos), repeated 0/1 (1 ONLY if the reply repeated
        a previous line — it must not). note: 2-4 sentences, direct
        analyst voice: what happened, what it means, how Newton did
        (good/bad and why), what to do differently; mention searches
        used and what they changed, if any. skip_reason: short reason
        when skipping, empty when replying. searches_used: the queries
        you ran, comma-separated, or 'none'."""
        if state["closed"]:
            return "Already closed."
        answered = state["reply_written"]
        if answered and reply_quality is not None and reply_quality < 0:
            return ("ERROR: you wrote a reply but passed reply_quality -1. "
                    "Score the reply you wrote.")
        if not answered and (reply_quality is None or reply_quality >= 0):
            # Skips carry no reply quality; normalize silently.
            reply_quality = None
        full_note = note.strip() if note and note.strip() else "No note."
        if searches_used and searches_used.strip().lower() != "none":
            full_note += f"\n[Searches used: {searches_used.strip()}]"
        conn = _db()
        try:
            conv = conn.execute(
                "SELECT id FROM conversations WHERE server = ? AND channel = ?"
                " AND ended_utc IS NULL", (server, channel)).fetchone()
            conv_id = conv[0] if conv else None
            if answered:
                newton_db.mark_interaction(conn, iid, "answered",
                                           reply_text=state["reply_text"])
            else:
                newton_db.mark_interaction(
                    conn, iid, "skipped",
                    skip_reason=(skip_reason.strip() or "agent skipped"))
            nid = newton_db.write_interaction_note(
                conn, server, channel,
                "answered" if answered else "skipped", full_note,
                interaction_id=iid, conversation_id=conv_id,
                trigger_msg_id=interaction["trigger_msg_id"],
                trigger_sender=interaction["trigger_sender"],
                engagement=max(0, min(100, int(engagement))),
                detection_risk=max(0, min(100, int(detection_risk))),
                reply_quality=(None if reply_quality is None else
                               max(0, min(100, int(reply_quality)))),
                heat=max(0, min(1000, int(heat))),
                repeated=1 if repeated else 0, source="brain")
        finally:
            conn.close()
        state["closed"] = True
        state["decision"] = "answered" if answered else "skipped"
        return (f"Closed as {state['decision']} (note id {nid}). "
                "Your final message must be a one-line summary, not the reply.")

    tools = [read_thread_context, read_recent_outbound, web_search,
             write_outbox_reply, close_interaction]
    return tools, state


REACT_TEMPLATE = """{soul}

## TOOLS AND WORKFLOW (read this after SOUL.md — it tells you HOW to work)

You have access to the following tools:

{{tools}}

Use the following format:

Question: the input question you must answer
Thought: you should always think about what to do
Action: the action to take, should be one of [{{tool_names}}]
Action Input: the input to the action
Observation: the result of the action
... (this Thought/Action/Action Input/Observation can repeat N times)
Thought: I now know the final answer
Final Answer: a one-line summary of what you did (replied or skipped, and why) — NOT the reply itself

Rules for the workflow, in order:
1. FIRST call read_thread_context for the trigger's channel, THEN call read_recent_outbound for the same channel. Never decide blind.
2. Search (web_search) ONLY when it genuinely helps: a factual or tech question, verifying a claim before you argue it, or giving help with facts instead of bluffing. Max 2 searches. Never search for anything about a person or this project.
3. Decide reply or skip from SOUL.md: reply only when addressed to Newton or genuinely in a thread he is part of AND you have a fresh, relevant, specific thing to say. Otherwise skip — silence beats a bad reply.
4. If replying, call write_outbox_reply (it refuses repeats — if it refuses, write something fresh). Then ALWAYS finish with close_interaction, exactly once, as your LAST step. If skipping, go straight to close_interaction with reply_quality -1.
5. Your Final Answer is a one-line summary, never the reply text itself.

Begin!

Question: {{input}}
Thought:{{agent_scratchpad}}"""


def run_agent(interaction):
    """Run one ReAct agent for a pending interaction dict (id, server,
    channel, trigger_sender, trigger_text). Returns a result dict:
    {closed, decision, reply, prompt_tokens, completion_tokens, cost_usd,
    llm_calls, searches}. Raises on surrogate/auth failure (caller
    leaves the interaction pending). If the agent finishes without
    closing, closed=False and the caller marks it skipped."""
    soul = load_soul()
    # Privacy: the assembled prompt must not contain owner identifiers.
    trigger_line = (f"{interaction['trigger_sender']}: "
                    f"{interaction['trigger_text']}")
    if _DENY.search(trigger_line):
        # The trigger itself trips the denylist: refuse without calling.
        raise ValueError("privacy denylist matched in trigger — refusing")
    surrogate = fetch_surrogate()
    from langchain_openai import ChatOpenAI
    import httpx
    http_client = httpx.Client(follow_redirects=False, timeout=60)
    llm = ChatOpenAI(model=MODEL, temperature=0.9, max_tokens=800,
                     base_url=API_BASE, api_key=surrogate,
                     http_client=http_client)
    tools, state = build_tools(interaction)
    from langchain_classic.agents import create_react_agent, AgentExecutor
    from langchain_classic.prompts import PromptTemplate
    prompt = PromptTemplate(
        template=REACT_TEMPLATE.format(soul=soul),
        input_variables=["input", "agent_scratchpad", "tools", "tool_names"])
    agent = create_react_agent(llm, tools, prompt)
    handler = TokenUsageHandler()
    executor = AgentExecutor(agent=agent, tools=tools, verbose=False,
                             max_iterations=MAX_ITERATIONS,
                             max_execution_time=MAX_EXECUTION_TIME_S,
                             handle_parsing_errors=True,
                             callbacks=[handler])
    human_input = (
        f"Server: {interaction['server']}\n"
        f"Channel: {interaction['channel']}\n"
        f"Trigger from {interaction['trigger_sender']}: "
        f"{interaction['trigger_text']}\n\n"
        "Follow the workflow: read thread context, read recent outbound, "
        "search only if it genuinely helps (max 2), decide reply or skip, "
        "write the reply if replying, and ALWAYS finish with "
        "close_interaction exactly once.")
    if _DENY.search(human_input):
        raise ValueError("privacy denylist matched in input — refusing")
    result = executor.invoke({"input": human_input})
    cost = (handler.prompt_tokens / 1e6 * PRICE_IN_PER_M +
            handler.completion_tokens / 1e6 * PRICE_OUT_PER_M)
    log_interaction(interaction["id"], interaction["server"],
                    interaction["channel"], handler.prompt_tokens,
                    handler.completion_tokens, cost, handler.calls,
                    state["decision"], list(state["searches"]))
    return {
        "closed": state["closed"],
        "decision": state["decision"],
        "reply": state["reply_text"],
        "prompt_tokens": handler.prompt_tokens,
        "completion_tokens": handler.completion_tokens,
        "cost_usd": round(cost, 6),
        "llm_calls": handler.calls,
        "searches": list(state["searches"]),
        "final": (result.get("output") or "")[:300],
    }
