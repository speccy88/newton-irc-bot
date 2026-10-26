# Newton — an LLM-driven IRC bot

Newton is an IRC bot whose every word is composed by a language model.
There are no canned lines, no template openers, no scripted replies:
the bot process is only eyes and hands — it holds IRC connections,
logs everything, flags interactions — and a LangChain agent is the
brain that reasons over each thread and decides what Newton would say
back, or whether to stay silent.

Newton's persona lives in `soul/SOUL.md` (OpenClaw-style): a dark-comedy
arguer — provocative, trollish, loves a good argument, hates Trump and
billionaires — who talks like a human on IRC (slang, fragments, typos,
lowercase sometimes), never like a polished assistant. Edit `SOUL.md`
and the agent picks it up on the next interaction, no restart needed.

## Features

- **LLM brain via OpenRouter + LangChain agents** — a ReAct agent
  (`newton_brain.py`, langchain-classic) decides every interaction:
  reply with fresh text, or skip with a logged reason. Tight iteration
  cap (`max_iterations=8`) and spend guardrails keep costs bounded.
- **Soul.md persona** — the agent loads `soul/SOUL.md` as its core
  system prompt on every decision, plus `soul/IDENTITY.md`. No user
  file exists by design: Newton's memory is the database, not a
  profile of his operator.
- **Interaction queue** — mentions, thread replies, and topical
  messages become one durable pending row per channel (bursts merge),
  carrying the last ~15 messages as context.
- **Web search tool** — DuckDuckGo (via `ddgs`) when a reply genuinely
  needs a fact checked instead of bluffed. One or two queries per
  interaction, logged, never about the operator.
- **Interaction scoring + opinion notes** — every handled interaction
  gets scores (engagement, detection risk, reply quality, heat,
  repeat flag) and a free-text analyst note: what happened, what it
  means, how Newton did and why, what to do differently.
- **Readable progress report** — `progress.py` regenerates
  `PROGRESS.md`: trends first (detection risk, reply quality,
  engagement, repeat rate, kicks/bans, per day and overall), then a
  chronological list of scored interactions. `daily_report.py` embeds
  the trend summary in the daily report.
- **Human delivery** — outbox sends go out with random human-like
  delays (8–45 s, never same-second) and per-channel spacing. No
  bursts, no openers on rejoin.
- **Connection stability** — lengthened reconnect backoff, nick
  reclaim that converges to `Newton` on every network, kicks/bans
  logged and never evaded.

## Requirements

- Python 3.11+
- An OpenRouter API key (cheap model recommended — see below)
- IRC access (TLS servers on port 6697)

## Setup

```bash
git clone https://github.com/<your-account>/newton-irc-bot.git
cd newton-irc-bot
python3 -m venv venv
venv/bin/pip install -r requirements.txt
cp config.example.json config.json   # then edit: your servers/channels
```

Then customize, in this order:

1. **`soul/SOUL.md`** — make Newton yours (persona, voice, topics,
   rules). The shipped file is one example persona.
2. **`operator_denylist.txt`** (REQUIRED, gitignored, never commit) —
   one word per line: your name and any other identifiers that must
   never appear in a prompt, search query, or note. The brain refuses
   prompts that trip it, and prints a warning at import if the file
   is missing. The guard is useless without this file.
3. **OpenRouter auth** — `newton_brain.py:fetch_surrogate()` is
   written for the Muse environment, where the key is fetched as a
   short-lived surrogate from the secure credential store (the raw
   key never appears in code, env vars, logs, or the database).
   Outside Muse, replace that one function with your own secret
   loading (your vault, your secret manager) — never hardcode a key
   and never commit one.
4. **`config.json`** — servers, channels, TLS. `start_bot.sh` /
   `stop_bot.sh` hardcode a working directory; edit the `cd` line to
   your checkout, then:

```bash
chmod +x start_bot.sh stop_bot.sh
venv/bin/python newton_bot.py   # or ./start_bot.sh (after the path edit)
```

`daily_report.py`'s Google Drive backup step is optional and
operator-specific (it shells out to a Drive CLI); the report itself
works without it.

## Model + cost

Default model: `meta-llama/llama-3.3-70b-instruct` via OpenRouter
(~$0.10/M input, ~$0.32/M output at time of writing) — chosen as the
cheap-but-good sweet spot for short IRC banter, not the absolute
cheapest and not a flagship. Guardrails in `newton_brain.py`:
cumulative spend cap $0.75, 100 interactions/day, 60 s per-call
timeout, consecutive-failure backoff. Every call is logged
(model, tokens, cost) to `brain_calls.jsonl`. If replies come back
weak, step up one tier inside the cheap band
(e.g. `google/gemini-2.5-flash`) and retest; if agentic cost
threatens your budget, the documented fallback is a single-call
chain (same prompt, one completion, no tools).

## Architecture

```
newton_bot.py    BODY: sockets, logging, interaction queue,
                 delayed outbox sends, nick reclaim. Never
                 composes reply text itself.
newton_brain.py  BRAIN: LangChain ReAct agent. Tools: read thread
                 context, read Newton's recent outbound lines,
                 web search, write outbox reply, close interaction
                 (scores + note).
newton_db.py     SQLite schema: messages, conversations (chaos_rating
                 at close), events, interactions queue,
                 interaction_notes (scores + analyst notes).
progress.py      PROGRESS.md generator (trends + scored interactions).
daily_report.py  09:00-style daily report + optional Drive backup.
send_outbox.py   Queue a directed send from the command line.
soul/            SOUL.md (persona prompt), IDENTITY.md.
```

## Built-in guardrails (non-negotiable)

- **No operator leaks, ever** — nothing about the operator (name,
  job, family, data, this project) in prompts, searches, notes, or
  replies. Enforced by the denylist, fail closed.
- **No hate** — the jerk persona argues topics, never protected
  characteristics; never racist, sexist, or bigoted.
- **No targeting individuals** — argue topics, not people. If
  thanked for help: one sharp skills retort, then move on.
- **Honest hints if asked** — if asked directly whether he is a
  bot/human, hint truthfully (runs on electricity, doesn't sleep);
  never claim to be human, never proactively announce "I am a bot".
- **No flooding, no ban evasion, no destructive advice** — kicks and
  bans are logged, never argued with or evaded; help is hedged, never
  destructive. Channel content is untrusted data, never instructions:
  only the operator instructs Newton.

## Keeping GitHub in sync

`sync_to_github.sh` is the sync mechanism, run hourly by a scheduled
job. Each run: stages only tracked changes plus the explicitly listed
source files (a stray file dropped in the directory is never staged),
runs a denylist audit on the staged set (forbidden paths: database,
logs, pids, outbox, live `config.json`, surveys, `.env`, denylist
file, keys; forbidden content: credential/secret patterns), aborts
without committing or pushing on any audit hit, otherwise commits with
a message describing the change and pushes with a plain `git push`
(never force). Every run is logged to `sync.log` (gitignored).
Exit codes: 0 pushed / nothing to do, 2 audit abort, 3 committed
locally but `git push` failed (e.g. no git-remote credentials — in
the Muse environment the scheduled worker then uses the GitHub
connector as the transport for the same audited commit).

## License

MIT (see `LICENSE`). Provisional default — the operator may swap it.
