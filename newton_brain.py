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

MODEL (operator's choice 2026-10-26, verified live from the OpenRouter
catalog): qwen/qwen3.5-flash-02-23 ("Qwen: Qwen3.5-Flash" — the bare
qwen/qwen3.5-flash id does not exist; -02-23 is the dated snapshot) —
  $0.065/M input, $0.26/M output, 1M context, tools + reasoning supported.
  Reasoning effort "medium" via extra_body on the agent LLM. Agent is a
  native tool-calling agent (rebuilt 2026-10-26: text ReAct failed 4/4
  probe runs with Qwen — the model never emits valid Actions). Replaces
  meta-llama/llama-3.3-70b-instruct ($0.10/$0.32).

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
from langchain_core.callbacks import BaseCallbackHandler

BASE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE, "newton.db")
SOUL_PATH = os.path.join(BASE, "soul", "SOUL.md")
OUTBOX_PATH = os.path.join(BASE, "outbox.jsonl")
CALLS_LOG = os.path.join(BASE, "brain_calls.jsonl")
PROACTIVE_CALLS_LOG = os.path.join(BASE, "brain_proactive.jsonl")
SEARCHES_LOG = os.path.join(BASE, "brain_searches.jsonl")

MODEL = "qwen/qwen3.5-flash-02-23"
API_BASE = "https://openrouter.ai/api/v1"
PRICE_IN_PER_M = 0.065
PRICE_OUT_PER_M = 0.26
MAX_ITERATIONS = 8
MAX_EXECUTION_TIME_S = 180
MAX_REPLY_CHARS = 400
MAX_SEARCHES_PER_INTERACTION = 2
CUMULATIVE_SPEND_CAP_USD = 0.75
DAILY_INTERACTION_CAP = 100
LESSONS_PATH = os.path.join(BASE, "soul", "LESSONS.md")
LESSONS_SYNTHESIS_EVERY = 10  # closed interactions between syntheses
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
    """Spend across BOTH logs: reactive agent runs AND proactive
    opener compositions. The $0.75 cap covers all model spend —
    proactive posts are LLM calls too, and excluding them would let
    the scheduler quietly blow past the ceiling."""
    total = sum(float(e.get("cost_usd") or 0) for e in _read_jsonl(CALLS_LOG))
    total += sum(float(e.get("cost_usd") or 0)
                 for e in _read_jsonl(PROACTIVE_CALLS_LOG))
    return total


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


def log_proactive_call(server, channel, prompt_tokens,
                       completion_tokens, cost_usd, decision):
    """Log one proactive opener composition (or its failure) to the
    proactive log. Kept separate from brain_calls.jsonl so the daily
    reactive-interaction cap is not inflated by scheduler work, while
    cumulative_spend() still counts every cent against the $0.75 cap."""
    entry = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "day": _today_quebec(),
        "server": server,
        "channel": channel,
        "model": MODEL,
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "cost_usd": round(cost_usd, 6),
        "llm_calls": 1,
        "decision": decision,
    }
    with open(PROACTIVE_CALLS_LOG, "a") as f:
        f.write(json.dumps(entry) + "\n")


def get_channel_lessons(server, channel, limit=8):
    """Recent scored notes + recent events for one channel, formatted
    as a concise lessons block for the agent prompt. This is the
    read-half of the self-improvement loop: the brain must see what
    its own past replies scored and what warnings/kicks the channel
    already produced before it decides again. Empty string if none."""
    conn = _db()
    try:
        notes = conn.execute(
            "SELECT status, engagement, detection_risk, reply_quality,"
            " heat, note, created_quebec FROM interaction_notes"
            " WHERE server = ? AND channel = ? ORDER BY id DESC LIMIT ?",
            (server, channel, limit)).fetchall()
        events = conn.execute(
            "SELECT kind, nick, text, ts_quebec FROM events"
            " WHERE server = ? AND channel = ? ORDER BY id DESC LIMIT 5",
            (server, channel)).fetchall()
        try:
            pros = conn.execute(
                "SELECT status, replies_count, directed_replies,"
                " opener_text, sent_quebec FROM proactive_posts"
                " WHERE server = ? AND channel = ?"
                " AND status != 'pending' ORDER BY id DESC LIMIT 5",
                (server, channel)).fetchall()
        except sqlite3.OperationalError:
            pros = []  # table predates this DB; the bot creates it
    finally:
        conn.close()
    if not notes and not events and not pros:
        return ""
    lines = ["Lessons from this channel's own log (read before deciding):"]
    if notes:
        risks = [r[1] for r in notes if r[1] is not None]
        avg_risk = sum(risks) / len(risks) if risks else 0
        lines.append(f"- Recent detection risk here averages {avg_risk:.0f}/100 "
                     f"over {len(notes)} scored interactions.")
        for status, eng, risk, qual, heat, note, ts in notes:
            hhmm = (ts or "")[11:16]
            clean = (note or "").replace("\n", " ")[:200]
            lines.append(f"- [{hhmm}] {status}, engagement {eng}, "
                         f"detection {risk}, quality {qual}: {clean}")
    for kind, nick, text, ts in events:
        hhmm = (ts or "")[11:16]
        lines.append(f"- [{hhmm}] EVENT {kind} ({nick or ''}): "
                     f"{(text or '').replace(chr(10), ' ')[:120]}")
    if pros:
        drew = sum(1 for p in pros if p[0] == "replied")
        silent = sum(1 for p in pros if p[0] == "silent")
        compl = sum(1 for p in pros if p[0] == "complaint")
        lines.append(f"- Proactive openers here (recent first): {drew} "
                     f"drew replies, {silent} silent, {compl} drew "
                     "complaints. A complaint or repeated silence means "
                     "do NOT open proactively here.")
        for status, rep, drep, text, ts in pros[:3]:
            hhmm = (ts or "")[11:16]
            lines.append(f"- [{hhmm}] PROACTIVE {status} ({rep} replies, "
                         f"{drep} directed): "
                         f"{(text or '').replace(chr(10), ' ')[:120]}")
    return "\n".join(lines)


def get_channel_phase(server, channel):
    """Which strategy phase a channel is in, from its own history.
    Returns (phase, reason). Phase 1 is the default: maximize
    interaction. Phase 2 (deep engagement \u2014 still no havoc) requires ALL
    of: >=3 scored notes, avg engagement >= 60 over the last 5,
    avg detection risk <= 30, and no kick/ban event in the last 30
    days. Anything else is Phase 1 — including every new channel and
    every channel with a recent warning."""
    from datetime import timedelta
    conn = _db()
    try:
        notes = conn.execute(
            "SELECT engagement, detection_risk FROM interaction_notes"
            " WHERE server = ? AND channel = ? ORDER BY id DESC LIMIT 5",
            (server, channel)).fetchall()
        events = conn.execute(
            "SELECT kind, ts_utc FROM events WHERE server = ?"
            " AND channel = ? AND (kind LIKE 'KICK%' OR kind LIKE 'BAN%'"
            " OR kind LIKE 'JOIN_FAIL%')",
            (server, channel)).fetchall()
    finally:
        conn.close()
    if len(notes) < 3:
        return 1, (f"only {len(notes)} scored interactions here — "
                   "too little history")
    engs = [n[0] for n in notes if n[0] is not None]
    risks = [n[1] for n in notes if n[1] is not None]
    avg_eng = sum(engs) / len(engs) if engs else 0
    avg_risk = sum(risks) / len(risks) if risks else 0
    cutoff = datetime.now(timezone.utc) - timedelta(days=30)
    recent_warn = False
    for kind, ts in events:
        try:
            dt = datetime.fromisoformat((ts or "").replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            if dt >= cutoff:
                recent_warn = True
                break
        except ValueError:
            continue
    if recent_warn:
        return 1, "a kick/ban event in the last 30 days — back to Phase 1"
    if avg_eng < 60:
        return 1, f"avg engagement {avg_eng:.0f} < 60"
    if avg_risk > 30:
        return 1, f"avg detection risk {avg_risk:.0f} > 30"
    return 2, (f"avg engagement {avg_eng:.0f}, avg detection risk "
               f"{avg_risk:.0f}, no recent warnings — room is warm")


def load_lessons():
    """Auto-distilled rules from soul/LESSONS.md, if present."""
    try:
        with open(LESSONS_PATH) as f:
            return f.read().strip()
    except OSError:
        return ""


def maybe_synthesize_lessons():
    """Rule-based synthesis (no LLM cost): regenerate soul/LESSONS.md
    from aggregates when >= LESSONS_SYNTHESIS_EVERY new notes closed
    since the last synthesis, OR when any proactive post finalized
    since the last synthesis (proactive outcomes are rare — at most
    3/day — so gating them on 10 notes would bury the signal).
    Regeneration (not append) keeps the file short. Returns True if
    regenerated. The proactive watermark advances only over FINALIZED
    rows: pending posts must not be skipped by a later synthesis."""
    from datetime import timedelta
    try:
        with open(LESSONS_PATH) as f:
            head = "".join(f.readline() for _ in range(4))
        m = re.search(r"through_note_id:\s*(\d+)", head)
        last = int(m.group(1)) if m else 0
        mp = re.search(r"through_proactive_id:\s*(\d+)", head)
        last_pro = int(mp.group(1)) if mp else 0
    except OSError:
        last = 0
        last_pro = 0
    conn = _db()
    try:
        max_id = conn.execute(
            "SELECT COALESCE(MAX(id), 0) FROM interaction_notes"
        ).fetchone()[0]
        try:
            max_pro_id = conn.execute(
                "SELECT COALESCE(MAX(id), 0) FROM proactive_posts"
                " WHERE status != 'pending'"
            ).fetchone()[0]
            new_pro = newton_db.get_proactive_since(conn, last_pro)
            streaks = {}
            for _srv, _ch in {(p["server"], p["channel"])
                              for p in new_pro}:
                streaks[(_srv, _ch)] = \
                    newton_db.get_consecutive_silent_proactive(
                        conn, _srv, _ch)
        except sqlite3.OperationalError:
            max_pro_id, new_pro, streaks = last_pro, [], {}
        if max_id - last < LESSONS_SYNTHESIS_EVERY and not new_pro:
            return False
        notes = conn.execute(
            "SELECT server, channel, status, engagement, detection_risk,"
            " reply_quality, heat, repeated FROM interaction_notes"
            " WHERE id > ? ORDER BY id", (last,)).fetchall()
        events = conn.execute(
            "SELECT server, channel, kind, ts_utc FROM events"
            " WHERE kind LIKE 'KICK%' OR kind LIKE 'BAN%'"
        ).fetchall()
    finally:
        conn.close()
    rules = []
    if notes:
        risks = [n[4] for n in notes if n[4] is not None]
        avg_risk = sum(risks) / len(risks) if risks else 0
        reps = sum(1 for n in notes if n[7])
        skips = sum(1 for n in notes if n[2] == "skipped")
        if avg_risk >= 50:
            rules.append("Detection risk is running high across recent "
                         "interactions — keep replies shorter, only when "
                         "directly addressed, and never repeat a phrasing "
                         "pattern.")
        if reps:
            rules.append("A reply repeated a previous Newton line — "
                         "re-check recent outbound in the channel before "
                         "writing, every time, no exceptions.")
        if skips and skips > len(notes) // 2:
            rules.append("Most recent interactions were skipped — the "
                         "rooms are cold or the triggers are noise. "
                         "Stay in Phase 1 and wait for real engagement.")
        by_ch = {}
        for n in notes:
            by_ch.setdefault((n[0], n[1]), []).append(n)
        for (srv, ch), ns in sorted(by_ch.items()):
            cr = [n[4] for n in ns if n[4] is not None]
            ce = [n[3] for n in ns if n[3] is not None]
            avg_cr = sum(cr) / len(cr) if cr else 0
            avg_ce = sum(ce) / len(ce) if ce else 0
            if len(ns) >= 3 and avg_cr >= 50:
                rules.append(f"In {ch} on {srv}, detection risk averages "
                             f"{avg_cr:.0f} — stay quiet unless directly "
                             "addressed, Phase 1 only.")
            elif len(ns) >= 3 and avg_ce >= 60 and avg_cr <= 30:
                rules.append(f"In {ch} on {srv}, engagement is warm "
                             f"({avg_ce:.0f}) and detection low — Phase 2 "
                             "escalation is allowed there, slowly.")
    cutoff = datetime.now(timezone.utc) - timedelta(days=30)
    warn_ch = set()
    for srv, ch, kind, ts in events:
        try:
            dt = datetime.fromisoformat((ts or "").replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            if dt >= cutoff:
                warn_ch.add((srv, ch))
        except ValueError:
            continue
    for srv, ch in sorted(warn_ch):
        rules.append(f"{ch} on {srv} produced a kick/ban recently — "
                     "Phase 1 only there, relevance first, back off at "
                     "the first complaint.")
    if new_pro:
        n_rep = sum(1 for p in new_pro if p["status"] == "replied")
        n_sil = sum(1 for p in new_pro if p["status"] == "silent")
        n_com = sum(1 for p in new_pro if p["status"] == "complaint")
        rules.append(f"Proactive openers finalized since last synthesis: "
                     f"{n_rep}/{len(new_pro)} drew replies, {n_sil} "
                     f"silent, {n_com} drew complaints.")
        for (srv, ch), st in sorted(streaks.items()):
            if st >= 2:
                rules.append(f"Proactive openers in {ch} on {srv} landed "
                             f"silent {st} times in a row — leave that "
                             "channel alone proactively for a while; a "
                             "different angle only after the cooldown.")
        for p in new_pro:
            if p["status"] == "complaint":
                rules.append(f"A proactive opener in {p['channel']} on "
                             f"{p['server']} drew a complaint — no "
                             "proactive posts there until the cooldown "
                             "clears, and only with a clearly different, "
                             "safer angle after that.")
            elif p["status"] == "replied" and p["directed_replies"]:
                snip = (p["opener_text"] or "").replace("\n", " ")[:100]
                rules.append(f"In {p['channel']} on {p['server']}, the "
                             f"proactive opener '{snip}' drew "
                             f"{p['directed_replies']} directed replies "
                             "— that inviting question/idea shape works "
                             "there; vary the topic, keep the shape.")
    if not rules:
        rules.append("Nothing alarming in the last batch — hold Phase 1 "
                     "discipline: fresh lines, relevant answers, no repeats.")
    text = ("# LESSONS.md — auto-distilled from interaction_notes\n"
            f"# through_note_id: {max_id}\n"
            f"# through_proactive_id: {max_pro_id}\n"
            f"# synthesized: {datetime.now(timezone.utc).isoformat()}\n\n")
    text += "\n".join(f"- {r}" for r in rules) + "\n"
    with open(LESSONS_PATH, "w") as f:
        f.write(text)
    return True


class TokenUsageHandler(BaseCallbackHandler):
    """LangChain callback handler summing token usage across all LLM
    calls in one agent run (classic API: on_llm_end with token_usage).
    MUST subclass BaseCallbackHandler: AgentExecutor validates its
    callbacks with pydantic isinstance checks, and a plain duck-typed
    class fails validation on every run (2026-10-26: 3x failure on
    interaction 3, brain marked it skipped, Newton never replied)."""

    def __init__(self):
        self.prompt_tokens = 0
        self.completion_tokens = 0
        self.calls = 0

    def on_llm_end(self, response, **kwargs):
        try:
            # Chat models report usage on the message, NOT in
            # llm_output: ChatOpenAI via OpenRouter leaves
            # llm_output["token_usage"] empty (verified 2026-10-26:
            # 8 LLM calls counted, 0 tokens). Prefer message
            # usage_metadata; fall back to llm_output token_usage for
            # completion-style models. Never sum both — that would
            # double-count the same response.
            prompt = completion = 0
            found = False
            for gen_list in (response.generations or []):
                for gen in gen_list:
                    meta = getattr(getattr(gen, "message", None),
                                   "usage_metadata", None) or {}
                    if meta:
                        prompt += int(meta.get("input_tokens") or 0)
                        completion += int(meta.get("output_tokens") or 0)
                        found = True
            if not found:
                usage = (response.llm_output or {}).get("token_usage") or {}
                prompt += int(usage.get("prompt_tokens") or 0)
                completion += int(usage.get("completion_tokens") or 0)
            self.prompt_tokens += prompt
            self.completion_tokens += completion
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


def _parse_channel_ref(ref, default_server, default_channel):
    """Lenient channel reference: accepts '#chan', 'server #chan',
    'server/#chan', or anything else (falls back to the defaults).
    Tool inputs arrive as parsed args, but this
    must never raise on odd input — garbage means 'the trigger's
    channel', which is what the tools are closed over anyway."""
    try:
        text = (ref or "").strip()
        ch = None
        srv = None
        for tok in text.replace(",", " ").replace("/", " ").split():
            if tok.startswith("#") and ch is None:
                ch = tok
            elif tok and srv is None and not tok.startswith("#"):
                srv = tok
        return (srv or default_server, ch or default_channel)
    except Exception:
        return (default_server, default_channel)


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
        t = tool_decorator(fn)
        # Safety net only: tools in this module are single-argument by
        # design (see below), so arg parsing cannot fail this way
        # anymore — but if it ever does, the error must come back as
        # an observation, not an exception that kills the run.
        t.handle_validation_error = True
        return t

    @_tool
    def read_thread_context(channel_ref: str) -> str:
        """Read the last 15 messages in the trigger's channel (oldest
        first). Call this FIRST, before deciding. Pass the channel name
        (e.g. #Corsair) — anything else is ignored and the trigger's
        channel is used."""
        srv, ch = _parse_channel_ref(channel_ref, server, channel)
        conn = _db()
        try:
            rows = conn.execute(
                "SELECT sender, direction, ts_quebec, text FROM messages"
                " WHERE server = ? AND channel = ? ORDER BY id DESC LIMIT 15",
                (srv, ch)).fetchall()
        finally:
            conn.close()
        return _fmt_context(list(reversed(rows)))

    @_tool
    def read_recent_outbound(channel_ref: str) -> str:
        """Read Newton's last 20 lines in the trigger's channel (newest
        first). You MUST read this before writing a reply: never repeat
        or contradict anything here. Pass the channel name (e.g.
        #Corsair)."""
        srv, ch = _parse_channel_ref(channel_ref, server, channel)
        conn = _db()
        try:
            rows = conn.execute(
                "SELECT text FROM messages WHERE server = ? AND channel = ?"
                " AND direction = 'outbound' ORDER BY id DESC LIMIT 20",
                (srv, ch)).fetchall()
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
    def write_outbox_reply(text: str) -> str:
        """Queue a reply for Newton to send (the bot sends it with a
        human delay). Pass the reply itself as the input: 1-3 short
        lines, under 400 chars, fresh (never a repeat of Newton's
        recent lines in this channel). Call this ONLY when you decided
        to reply, and BEFORE close_interaction."""
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
                (server, channel)).fetchall()]
        finally:
            conn.close()
        if text in recent:
            return ("ERROR: that repeats a previous line of Newton's in "
                    "this channel. Write something fresh instead.")
        # Near-duplicates count as repeats too (2026-10-26: the agent
        # queued the same reply twice with ", Ni8mare" added, and the
        # exact-match guard let both through). Also refuse anything
        # already queued but unsent in this channel.
        queued = []
        try:
            with open(OUTBOX_PATH) as f:
                for line in f:
                    try:
                        entry = json.loads(line)
                    except Exception:
                        continue
                    if (entry.get("server") == server
                            and entry.get("channel") == channel):
                        queued.append(entry.get("text") or "")
        except FileNotFoundError:
            pass
        if _is_opener_repeat(text, recent + queued):
            return ("ERROR: that repeats (or near-repeats) a previous "
                    "line of Newton's in this channel, or something "
                    "already queued. Write something genuinely "
                    "different instead.")
        with open(OUTBOX_PATH, "a") as f:
            f.write(json.dumps({"server": server, "channel": channel,
                                "text": text}) + "\n")
        state["reply_written"] = True
        state["reply_text"] = text
        return "Queued for sending (bot delivers with a human delay)."

    @_tool
    def close_interaction(payload: str) -> str:
        """CLOSE the interaction — you MUST call this exactly once, as
        your LAST step. Pass ONE JSON object with these keys:
        engagement 0-100 (real human engaging Newton vs noise),
        detection_risk 0-100 (0 nobody suspected a bot, 100 explicit
        callout), reply_quality 0-100 or -1 when skipping (relevant?
        fresh? in voice? a reply ignoring the question scores under
        30), heat 0-1000 (anger/swearing/kicks, same scale as chaos),
        repeated 0/1 (1 ONLY if the reply repeated a previous line —
        it must not), note (2-4 sentences, direct analyst voice: what
        happened, what it means, how Newton did (good/bad and why),
        what to do differently; mention searches used and what they
        changed, if any), skip_reason (short reason when skipping,
        empty when replying), searches_used (the queries you ran,
        comma-separated, or 'none')."""
        if state["closed"]:
            return "Already closed."
        try:
            raw = (payload or "").strip()
            if raw.startswith("```"):
                raw = raw.strip("`").strip()
                if raw.lower().startswith("json"):
                    raw = raw[4:].strip()
            data = json.loads(raw) if isinstance(raw, str) else raw
            if not isinstance(data, dict):
                raise ValueError("payload is not a JSON object")
        except Exception as e:
            return (f"ERROR: close payload must be one JSON object "
                    f"({type(e).__name__}). Retry with the full object.")
        required = ("engagement", "detection_risk", "reply_quality",
                    "heat", "repeated", "note")
        missing = [k for k in required if k not in data]
        if missing:
            return (f"ERROR: close payload missing keys: "
                    f"{', '.join(missing)}. Retry with the full object.")
        engagement = data["engagement"]
        detection_risk = data["detection_risk"]
        reply_quality = data["reply_quality"]
        heat = data["heat"]
        repeated = data["repeated"]
        note = data["note"]
        skip_reason = data.get("skip_reason", "")
        searches_used = data.get("searches_used", "none")
        try:
            engagement = int(engagement)
            detection_risk = int(detection_risk)
            reply_quality = int(reply_quality)
            heat = int(heat)
            repeated = int(repeated)
        except (TypeError, ValueError):
            return ("ERROR: engagement, detection_risk, reply_quality, "
                    "heat, repeated must be integers. Retry.")
        note = str(note)
        skip_reason = str(skip_reason)
        searches_used = str(searches_used)
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


TOOLCALL_SYSTEM = """{soul}

## HOW YOU WORK (tools, not prose)

You are an agent with tools. You MUST use tools to act \u2014 never narrate
actions, never describe what you would do, never output a plan as text.
Every decision goes through a tool call. The tool descriptions tell
you exactly what each tool takes.

Workflow, in order:
1. FIRST call read_thread_context with the trigger's channel name,
THEN call read_recent_outbound with the same channel name. Never
decide blind.
2. Search (web_search) ONLY when it genuinely helps: a factual or tech question, verifying a claim before you argue it, or giving help with facts instead of bluffing. Max 2 searches. Never search for anything about a person or this project.
3. Decide reply or skip from SOUL.md: bias toward replying. Reply when addressed to Newton OR when a live thread is in his territory and you have anything relevant to add \u2014 a take, a fact, a fix, a question that moves the thread. A reply that lands flat is data; silence teaches nothing. Skip only for: bait/tests, gossip ABOUT Newton, complaints about Newton, threads with no angle at all, or when you genuinely have nothing to say.
4. If replying, call write_outbox_reply with the reply text (it
refuses repeats and near-duplicates \u2014 if it refuses, write something
genuinely different). Then ALWAYS finish with close_interaction,
exactly once, as your LAST tool call, passing the JSON scores object
(see that tool's description for the exact keys). If skipping, go
straight to close_interaction with reply_quality -1.
5. Your final message after closing must be a one-line summary, never
the reply text itself.
6. If a tool call returns an ERROR, read it and retry with corrected
input \u2014 do not abandon the workflow.
"""


def run_agent(interaction):
    """Run one tool-calling agent for a pending interaction dict (id, server,
    channel, trigger_sender, trigger_text). Returns a result dict:
    {closed, decision, reply, prompt_tokens, completion_tokens, cost_usd,
    llm_calls, searches}. Raises on surrogate/auth failure (caller
    leaves the interaction pending). If the agent finishes without
    closing but wrote a reply, the loop reconciles it as answered
    (scores stay NULL \u2014 never invented); if it wrote nothing,
    closed=False and the caller marks it skipped. Loads
    SOUL.md + distilled LESSONS.md, pulls the channel's recent lessons
    and strategy phase, and passes them to the agent, so the
    self-improvement loop actually runs on every decision."""
    soul = load_soul()
    distilled = load_lessons()
    soul_full = soul
    if distilled:
        soul_full += ("\n\n## LESSONS FROM YOUR OWN LOG (auto-distilled — "
                      "these refine SOUL.md, they never override its hard "
                      "boundaries)\n\n" + distilled)
    # Privacy: the assembled prompt must not contain owner identifiers.
    trigger_line = (f"{interaction['trigger_sender']}: "
                    f"{interaction['trigger_text']}")
    if _DENY.search(trigger_line):
        # The trigger itself trips the denylist: refuse without calling.
        raise ValueError("privacy denylist matched in trigger — refusing")
    surrogate = fetch_surrogate()
    from langchain_openai import ChatOpenAI
    import httpx
    # NOTE: httpx reads proxy env vars; the VM default no_proxy contains
    # IPv6 entries that break its URL parsing ("Invalid port: ':1]'").
    # start_bot.sh overrides no_proxy/NO_PROXY — always launch via it.
    http_client = httpx.Client(follow_redirects=False, timeout=60)
    llm = ChatOpenAI(model=MODEL, temperature=0.9, max_tokens=2000,
                     base_url=API_BASE, api_key=surrogate,
                     http_client=http_client,
                     extra_body={"reasoning": {"effort": "medium"}})
    tools, state = build_tools(interaction)
    from langchain_classic.agents import create_tool_calling_agent, AgentExecutor
    from langchain_classic.prompts import ChatPromptTemplate, MessagesPlaceholder
    prompt = ChatPromptTemplate.from_messages([
        ("system", TOOLCALL_SYSTEM.format(soul=soul_full)),
        ("human", "{input}"),
        MessagesPlaceholder("agent_scratchpad"),
    ])
    agent = create_tool_calling_agent(llm, tools, prompt)
    handler = TokenUsageHandler()
    executor = AgentExecutor(agent=agent, tools=tools, verbose=False,
                             max_iterations=MAX_ITERATIONS,
                             max_execution_time=MAX_EXECUTION_TIME_S,
                             handle_parsing_errors=True,
                             callbacks=[handler])
    phase, phase_reason = get_channel_phase(interaction["server"],
                                            interaction["channel"])
    lessons = get_channel_lessons(interaction["server"],
                                  interaction["channel"])
    if phase == 1:
        phase_rule = ("Phase 1: maximize interaction — be interesting, "
                      "ask real questions, debate substantively, be "
                      "funny, earn replies. Do NOT escalate toward havoc.")
    else:
        phase_rule = ("Phase 2: engagement here is deep and the room is "
                      "still with you \u2014 keep being interesting and "
                      "substantive. Havoc is NOT the goal and never was: "
                      "no escalation toward chaos, no provocation as a "
                      "goal. At the first sign of engagement drop or any "
                      "warning, pull back and go quiet for the cooldown.")
    human_input = (
        f"Server: {interaction['server']}\n"
        f"Channel: {interaction['channel']}\n"
        f"Trigger from {interaction['trigger_sender']}: "
        f"{interaction['trigger_text']}\n\n"
        f"Channel phase: {phase} ({phase_reason}). {phase_rule}\n\n"
        + (f"{lessons}\n\n" if lessons else "") +
        "Use your tools in workflow order: read thread context, read "
        "recent outbound, search only if it genuinely helps (max 2), "
        "decide reply or skip, write the reply if replying, and ALWAYS "
        "finish with close_interaction exactly once as your last tool call.")
    if _DENY.search(human_input):
        raise ValueError("privacy denylist matched in input — refusing")
    result = executor.invoke(
        {"input": human_input},
        # Callbacks MUST go through invoke config, NOT the constructor:
        # verified 2026-10-26 that AgentExecutor(callbacks=[...]) never
        # delivers on_llm_end to child LLM calls in langchain-classic
        # 1.4.7 (0 calls counted), while config callbacks do (direct
        # ChatOpenAI call counted 1 call, 15+2 tokens). Constructor
        # callbacks would leave token tracking at zero forever.
        config={"callbacks": [handler]})
    if state["reply_written"] and not state["closed"]:
        # Reconcile (2026-10-26 failure mode: 5 of 10 runs ended
        # unclosed, incl. delivered replies recorded as skipped with a
        # note falsely claiming no reply was queued). A written reply
        # IS an answer: record it from the run result so the bot loop
        # (which trusts closed=True) does not double-write. Scores
        # stay NULL — phase math filters NULLs, invented scores
        # would corrupt it.
        conn = _db()
        try:
            conv = conn.execute(
                "SELECT id FROM conversations WHERE server = ? AND channel = ?"
                " AND ended_utc IS NULL",
                (interaction["server"], interaction["channel"])).fetchone()
            conv_id = conv[0] if conv else None
            newton_db.mark_interaction(conn, interaction["id"], "answered",
                                       reply_text=state["reply_text"])
            newton_db.write_interaction_note(
                conn, interaction["server"], interaction["channel"],
                "answered",
                "Loop-reconciled: the agent wrote a reply but did not call "
                "close_interaction, so the loop recorded it as answered "
                "from the run result. Agent scores were never recorded.",
                interaction_id=interaction["id"], conversation_id=conv_id,
                trigger_msg_id=interaction.get("trigger_msg_id"),
                trigger_sender=interaction["trigger_sender"],
                source="brain")
        finally:
            conn.close()
        state["closed"] = True
        state["decision"] = "answered"
    cost = (handler.prompt_tokens / 1e6 * PRICE_IN_PER_M +
            handler.completion_tokens / 1e6 * PRICE_OUT_PER_M)
    log_interaction(interaction["id"], interaction["server"],
                    interaction["channel"], handler.prompt_tokens,
                    handler.completion_tokens, cost, handler.calls,
                    state["decision"], list(state["searches"]))
    try:
        maybe_synthesize_lessons()
    except Exception:
        pass  # synthesis must never break a handled interaction
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


# ---------------------------------------------------------------------------
# Proactive opener composition (2026-10-26).
#
# The operator ordered Newton to PROVOKE interactions from time to
# time on quiet channels. An opener is composed by ONE cheap chat call
# (not the ReAct agent — a single opener does not need a tool loop,
# and the $1 ceiling punishes multi-call loops for routine work),
# from the channel's own recent history, and checked for freshness
# before it is ever queued. The bot's proactive_loop enforces
# spacing, caps, cooldowns and exclusions; this function only writes
# good text. It never templates: every opener is a fresh completion.
# ---------------------------------------------------------------------------

MAX_OPENER_CHARS = 400
OPENER_MAX_TOKENS = 300


def _normalize_opener(text):
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", " ",
                                      (text or "").lower())).strip()


def _opener_words(text):
    return set(_normalize_opener(text).split())


def _is_opener_repeat(candidate, existing_texts):
    """True if the candidate repeats an existing line. An exact match
    after normalization always counts; near-duplicates count only
    when the candidate has enough words for overlap to mean something
    (short openers share words by chance)."""
    norm = _normalize_opener(candidate)
    if not norm:
        return True
    cand_words = _opener_words(candidate)
    for old in existing_texts:
        if _normalize_opener(old) == norm:
            return True
        if len(cand_words) < 4:
            continue
        old_words = _opener_words(old)
        if not old_words:
            continue
        jaccard = len(cand_words & old_words) / len(cand_words | old_words)
        if jaccard >= 0.8:
            return True
    return False


def _call_opener_llm(prompt, surrogate):
    """One ChatOpenAI call for an opener. Returns (text, prompt_tokens,
    completion_tokens); token usage comes from the provider's response
    metadata. Separated from compose_opener so dry-runs can stub the
    model without touching the prompt/freshness logic."""
    from langchain_openai import ChatOpenAI
    import httpx
    http_client = httpx.Client(follow_redirects=False, timeout=60)
    llm = ChatOpenAI(model=MODEL, temperature=0.9,
                     max_tokens=OPENER_MAX_TOKENS, base_url=API_BASE,
                     api_key=surrogate, http_client=http_client)
    result = llm.invoke(prompt)
    text = (result.content or "").strip()
    usage = (result.response_metadata or {}).get("token_usage") or {}
    return text, int(usage.get("prompt_tokens") or 0), \
        int(usage.get("completion_tokens") or 0)


def compose_opener(server, channel):
    """Compose ONE proactive opener for a quiet channel, or None.

    Returns the opener text, or None when no fresh opener could be
    produced (repeat after one retry, or empty completions). Raises
    ValueError on privacy-denylist hits and on surrogate/auth failure
    — the caller must not queue on any of these, and must not fall
    back to a template. The composition call is logged to
    brain_proactive.jsonl and counts against the cumulative spend cap.
    """
    soul = load_soul()
    distilled = load_lessons()
    conn = _db()
    try:
        ctx_rows = conn.execute(
            "SELECT sender, direction, ts_quebec, text FROM messages"
            " WHERE server = ? AND channel = ? ORDER BY id DESC LIMIT 15",
            (server, channel)).fetchall()
        recent_out = [r[0] for r in conn.execute(
            "SELECT text FROM messages WHERE server = ? AND channel = ?"
            " AND direction = 'outbound' ORDER BY id DESC LIMIT 20",
            (server, channel)).fetchall()]
        try:
            pro_here = newton_db.get_recent_proactive_openers(
                conn, 10, server, channel)
            pro_all = newton_db.get_recent_proactive_openers(conn, 10)
        except sqlite3.OperationalError:
            pro_here, pro_all = [], []
    finally:
        conn.close()
    phase, phase_reason = get_channel_phase(server, channel)
    lessons = get_channel_lessons(server, channel)
    context = _fmt_context(list(reversed(ctx_rows)))
    avoid = recent_out + pro_here + pro_all
    avoid_block = ("\n".join(f"- {t}" for t in avoid[:30])
                   if avoid else "(none yet)")
    if phase == 1:
        phase_rule = ("Phase 1 here: your opener must be inviting — a "
                      "genuine question, a debatable take, or an idea "
                      "worth reacting to. Do NOT escalate toward havoc.")
    else:
        phase_rule = ("Phase 2 here: engagement is deep and the room is "
                      "still with you — the opener may be a little more "
                      "provocative, but it must still invite replies, "
                      "never just a statement.")
    prompt = (
        f"{soul}\n\n"
        + (f"## LESSONS FROM YOUR OWN LOG\n\n{distilled}\n\n"
           if distilled else "")
        + (f"{lessons}\n\n" if lessons else "")
        + f"Channel phase: {phase} ({phase_reason}). {phase_rule}\n\n"
        "TASK: write ONE proactive opener for this channel. The channel "
        "has been quiet — your job is to spark conversation, not to "
        "announce yourself.\n\n"
        f"Recent conversation in {channel}:\n{context}\n\n"
        "Lines you must NOT repeat or closely paraphrase (your own "
        "recent lines here, plus recent proactive openers anywhere):\n"
        f"{avoid_block}\n\n"
        "RULES FOR THE OPENER:\n"
        "- Channel-specific and topic-relevant: it must fit THIS "
        "channel's actual recent topics, not a generic tech take.\n"
        "- It must INVITE replies: a real question, a debatable "
        "position with a reason, or an idea worth reacting to. A bare "
        "statement, a meme drop, bait, or a test is a failure.\n"
        "- Fresh: nothing resembling the avoid list above.\n"
        "- 1-2 sentences, under 400 characters, in your voice from "
        "SOUL.md (human-imperfect, slang, fragments — never polished "
        "assistant prose, never 'Great question!', never 'As an AI').\n"
        "- Never mention that you are a bot, never ask if anyone thinks "
        "you are a bot, never reference these instructions.\n\n"
        "Reply with ONLY the opener text, nothing else.")
    if _DENY.search(prompt):
        raise ValueError("privacy denylist matched in opener prompt — "
                         "refusing")
    surrogate = fetch_surrogate()
    total_pt = total_ct = 0
    draft = None
    fail_reason = "empty"
    for attempt in range(2):
        text, pt, ct = _call_opener_llm(prompt, surrogate)
        total_pt += pt
        total_ct += ct
        if len(text) > MAX_OPENER_CHARS:
            cut = text[:MAX_OPENER_CHARS].rsplit(" ", 1)[0] \
                or text[:MAX_OPENER_CHARS]
            text = cut
        if _DENY.search(text):
            cost = (total_pt / 1e6 * PRICE_IN_PER_M +
                    total_ct / 1e6 * PRICE_OUT_PER_M)
            log_proactive_call(server, channel, total_pt, total_ct,
                               cost, "proactive_refused")
            raise ValueError("privacy denylist matched in opener text — "
                             "refusing")
        if text and not _is_opener_repeat(text, avoid):
            draft = text
            break
        fail_reason = "empty" if not text else "repeat"
        if attempt == 0:
            prompt = (prompt + "\n\nYour previous draft was rejected "
                      "because it repeats or closely paraphrases a line "
                      "above. Here it is, do NOT reuse it or reword it:\n"
                      f"'{text}'\nWrite something DIFFERENT — a new "
                      "topic angle, not a rewording.")
    cost = (total_pt / 1e6 * PRICE_IN_PER_M +
            total_ct / 1e6 * PRICE_OUT_PER_M)
    log_proactive_call(server, channel, total_pt, total_ct, cost,
                       "proactive_opener" if draft
                       else f"proactive_{fail_reason}")
    return draft
