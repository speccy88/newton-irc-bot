#!/usr/bin/env python3
"""Newton IRC — SQLite logging + chaos scoring.

Database: ~/workspace/newton-irc/newton.db

Tables
------
messages:
    id          INTEGER PRIMARY KEY
    server      TEXT    -- network key, e.g. 'tilde', 'libera'
    channel     TEXT    -- e.g. '#bots'
    sender      TEXT    -- nick of the author
    direction   TEXT    -- 'outbound' (Newton) or 'inbound' (anyone else)
    text        TEXT    -- raw message text
    ts_utc      TEXT    -- ISO-8601 UTC timestamp
    ts_quebec   TEXT    -- America/Toronto wall-clock time (same instant)

conversations:
    id              INTEGER PRIMARY KEY
    server, channel TEXT
    started_utc, ended_utc     TEXT (ISO UTC)
    started_quebec, ended_quebec TEXT (America/Toronto wall time)
    message_count   INTEGER -- all messages in the thread
    participant_count INTEGER -- distinct nicks (incl. Newton)
    chaos_rating    INTEGER -- 0..1000, NULL until the conversation finishes
    evaluation      TEXT    -- human-readable assessment, NULL until finished

Conversation definition: one continuous exchange thread in a channel.
A new conversation starts after 30 minutes of inactivity in that channel,
or when Newton leaves the channel. When a conversation finishes, the
chaos scorer fills chaos_rating + evaluation.

Chaos scale (OBSERVATION metric, not a target):
    0    = no reaction at all (silence / ignored)
    1-250  = mild: a few replies, light disagreement
    251-500 = heated debate: fast back-and-forth, caps, exclamations
    501-750 = angry: insults, swearing, multiple people piling on
    751-1000 = absolute chaos: sustained fury, heavy swearing,
               kicks/bans, thread meltdown. 1000 requires people (or bots)
               getting really angry and swearing.

Scoring signals (computed ONLY from inbound messages, i.e. other people's
reactions — Newton's own words never inflate his own score):
    - profanity / anger-word hits
    - ALL-CAPS word ratio
    - exclamation-mark density
    - reply volume and speed (inbound msgs/minute while active)
    - kicks or bans targeting Newton
    - thread length / participant count
No single signal can max the score alone (except a kick/ban combined with
heavy swearing); the score is a clamped weighted sum.
"""
import re
import sqlite3
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

QUEBEC = ZoneInfo("America/Toronto")
DB_PATH_DEFAULT = "/home/hatch/workspace/newton-irc/newton.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    server TEXT NOT NULL,
    channel TEXT NOT NULL,
    sender TEXT NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('outbound', 'inbound')),
    text TEXT NOT NULL,
    ts_utc TEXT NOT NULL,
    ts_quebec TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_messages_server_channel_ts
    ON messages (server, channel, ts_utc);
CREATE TABLE IF NOT EXISTS conversations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    server TEXT NOT NULL,
    channel TEXT NOT NULL,
    started_utc TEXT NOT NULL,
    ended_utc TEXT,
    started_quebec TEXT NOT NULL,
    ended_quebec TEXT,
    message_count INTEGER NOT NULL DEFAULT 0,
    participant_count INTEGER NOT NULL DEFAULT 0,
    chaos_rating INTEGER CHECK (chaos_rating IS NULL OR (chaos_rating >= 0 AND chaos_rating <= 1000)),
    evaluation TEXT
);
CREATE INDEX IF NOT EXISTS idx_conversations_server_channel
    ON conversations (server, channel, started_utc);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    server TEXT NOT NULL,
    channel TEXT NOT NULL,
    kind TEXT NOT NULL,
    nick TEXT,
    text TEXT,
    ts_utc TEXT NOT NULL,
    ts_quebec TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS interactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    server TEXT NOT NULL,
    channel TEXT NOT NULL,
    trigger_msg_id INTEGER NOT NULL,
    trigger_sender TEXT NOT NULL,
    trigger_text TEXT NOT NULL,
    context_json TEXT NOT NULL DEFAULT '[]',
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'answered', 'skipped')),
    reply_text TEXT,
    skip_reason TEXT,
    created_utc TEXT NOT NULL,
    decided_utc TEXT
);
CREATE INDEX IF NOT EXISTS idx_interactions_pending
    ON interactions (status, server, channel);
"""

PROFANITY = [
    r"\bf+u+c+k+\b", r"\bs+h+i+t+\b", r"\bb+i+t+c+h+\b", r"\ba+s+s+h+o+l+e+\b",
    r"\bb+a+s+t+a+r+d+\b", r"\bd+a+m+n+\b", r"\bh+e+l+l+\b", r"\bc+r+a+p+\b",
    r"\bd+i+c+k+\b", r"\bp+r+i+c+k+\b", r"\bb+o+l+l+o+c+k+s+\b",
    r"\bwtf\b", r"\bstfu\b", r"\bgtfo\b", r"\bkys\b",
]
ANGER_WORDS = [
    r"\bidiot\b", r"\bmoron\b", r"\bstupid\b", r"\bdumb\b", r"\bshut up\b",
    r"\bshut the\b", r"\bhate you\b", r"\bget lost\b", r"\bscrew you\b",
    r"\bretard(ed)?\b", r"\bclown\b", r"\bpathetic\b", r"\btrash\b",
    r"\bgarbage\b", r"\bwrong\b.*\bwrong\b",
]
PROFANITY_RE = [re.compile(p, re.IGNORECASE) for p in PROFANITY]
ANGER_RE = [re.compile(p, re.IGNORECASE) for p in ANGER_WORDS]


def now_pair():
    """Return (ts_utc_iso, ts_quebec_iso) for the current instant."""
    now = datetime.now(timezone.utc)
    return now.isoformat(), now.astimezone(QUEBEC).isoformat()


def connect(db_path=DB_PATH_DEFAULT):
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)
    # Interaction notes live in NOTES_SCHEMA (defined below SCHEMA);
    # apply it here too so every connect guarantees the table exists.
    try:
        conn.executescript(NOTES_SCHEMA)
    except NameError:
        pass
    # Proactive posts live in PROACTIVE_SCHEMA (same pattern).
    try:
        conn.executescript(PROACTIVE_SCHEMA)
    except NameError:
        pass
    # Participation-gate telemetry lives in GATE_SCHEMA (same pattern).
    try:
        conn.executescript(GATE_SCHEMA)
    except NameError:
        pass
    # Participation-gate migration (2026-10-28): interactions.gate records
    # how a queued interaction passed the gate ('addressed',
    # 'continuation', 'latched-addressed'); NULL for pre-gate rows.
    cols = [r[1] for r in conn.execute("PRAGMA table_info(interactions)")]
    if "gate" not in cols:
        conn.execute("ALTER TABLE interactions ADD COLUMN gate TEXT")
    conn.commit()
    return conn


def log_message(conn, server, channel, sender, direction, text):
    ts_utc, ts_qc = now_pair()
    cur = conn.execute(
        "INSERT INTO messages (server, channel, sender, direction, text, ts_utc, ts_quebec)"
        " VALUES (?, ?, ?, ?, ?, ?, ?)",
        (server, channel, sender, direction, text, ts_utc, ts_qc),
    )
    # Keep live counts on open conversations for this server/channel accurate.
    conn.execute(
        "UPDATE conversations SET message_count = (SELECT COUNT(*) FROM messages"
        " WHERE server = ? AND channel = ? AND ts_utc >= conversations.started_utc"
        " AND (conversations.ended_utc IS NULL OR ts_utc <= conversations.ended_utc)),"
        " participant_count = (SELECT COUNT(DISTINCT sender) FROM messages"
        " WHERE server = ? AND channel = ? AND ts_utc >= conversations.started_utc"
        " AND (conversations.ended_utc IS NULL OR ts_utc <= conversations.ended_utc))"
        " WHERE server = ? AND channel = ? AND ended_utc IS NULL",
        (server, channel, server, channel, server, channel),
    )
    conn.commit()
    return ts_utc, ts_qc, cur.lastrowid


def log_event(conn, server, channel, kind, nick, text=""):
    ts_utc, ts_qc = now_pair()
    conn.execute(
        "INSERT INTO events (server, channel, kind, nick, text, ts_utc, ts_quebec)"
        " VALUES (?, ?, ?, ?, ?, ?, ?)",
        (server, channel, kind, nick, text, ts_utc, ts_qc),
    )
    conn.commit()


def _caps_ratio(texts):
    words = []
    for t in texts:
        words += re.findall(r"[A-Za-z]+", t)
    if not words:
        return 0.0
    caps = sum(1 for w in words if len(w) >= 3 and w.isupper())
    return caps / len(words)


def score_conversation(conn, conv_id):
    """Score a finished conversation from its inbound messages + events.

    Returns (rating:int 0..1000, evaluation:str). Does NOT write; the
    caller writes the result so scoring stays testable.
    """
    row = conn.execute(
        "SELECT server, channel, started_utc, ended_utc FROM conversations WHERE id = ?",
        (conv_id,),
    ).fetchone()
    if not row:
        return 0, "Conversation not found."
    server, channel, started, ended = row
    msgs = conn.execute(
        "SELECT sender, direction, text, ts_utc FROM messages"
        " WHERE server = ? AND channel = ? AND ts_utc >= ? AND (? IS NULL OR ts_utc <= ?)"
        " ORDER BY ts_utc",
        (server, channel, started, ended, ended),
    ).fetchall()
    inbound = [m for m in msgs if m[1] == "inbound"]
    outbound = [m for m in msgs if m[1] == "outbound"]
    inbound_texts = [m[2] for m in inbound]
    participants = len({m[0] for m in msgs})
    events = conn.execute(
        "SELECT kind, nick FROM events WHERE server = ? AND channel = ?"
        " AND ts_utc >= ? AND (? IS NULL OR ts_utc <= ?)",
        (server, channel, started, ended, ended),
    ).fetchall()
    kicks = sum(1 for k, n in events if k in ("KICK", "BAN") and n == "Newton")

    profanity_hits = sum(len(rx.findall(t)) for t in inbound_texts for rx in PROFANITY_RE)
    anger_hits = sum(len(rx.findall(t)) for t in inbound_texts for rx in ANGER_RE)
    caps = _caps_ratio(inbound_texts)
    excl = sum(t.count("!") for t in inbound_texts)
    excl_density = excl / max(1, len(inbound_texts))

    # reply speed: inbound messages per active minute
    if len(inbound) >= 2:
        t0 = datetime.fromisoformat(inbound[0][3])
        t1 = datetime.fromisoformat(inbound[-1][3])
        span_min = max(1.0, (t1 - t0).total_seconds() / 60.0)
        rate = len(inbound) / span_min
    else:
        rate = 0.0

    score = 0.0
    score += min(200, profanity_hits * 40)          # swearing: heavy weight
    score += min(120, anger_hits * 30)              # insults / anger words
    score += min(150, caps * 500)                   # caps ratio
    score += min(100, excl_density * 60)            # exclamation density
    score += min(150, rate * 15)                    # reply speed
    score += min(120, max(0, len(inbound) - 1) * 10)  # thread length
    score += min(80, max(0, participants - 2) * 20)  # pile-on factor
    score += kicks * 200                            # kick/ban: meltdown signal
    rating = max(0, min(1000, int(round(score))))

    if kicks:
        verdict = "meltdown — Newton got kicked/banned"
    elif rating >= 751:
        verdict = "absolute chaos — sustained fury and swearing"
    elif rating >= 501:
        verdict = "angry — insults and swearing, pile-on"
    elif rating >= 251:
        verdict = "heated debate — fast, loud, but not abusive"
    elif rating >= 1:
        verdict = "mild reaction — some replies, light disagreement"
    else:
        verdict = "no reaction — silence"

    evaluation = (
        f"{verdict}. Inbound messages: {len(inbound)}, Newton messages: {len(outbound)}, "
        f"participants: {participants}, profanity hits: {profanity_hits}, "
        f"anger-word hits: {anger_hits}, caps ratio: {caps:.2f}, "
        f"exclamations per message: {excl_density:.2f}, "
        f"reply rate: {rate:.1f}/min, kicks/bans: {kicks}."
    )
    return rating, evaluation


def open_conversation(conn, server, channel, started_utc=None, started_qc=None):
    # Participation-gate fix (2026-10-28): never leave two open windows
    # for one channel. The old code opened a fresh conversation on every
    # (re)join while the pre-restart one stayed open, so every later
    # count query double-counted the overlap (266 conversations claiming
    # 4,761 messages vs 956 real). Close priors first; their end abuts
    # this start, so windows never overlap again.
    for (oid,) in conn.execute(
        "SELECT id FROM conversations WHERE server = ? AND channel = ?"
        " AND ended_utc IS NULL",
        (server, channel),
    ).fetchall():
        try:
            finish_conversation(conn, oid)
        except Exception:
            pass
    if started_utc is None:
        started_utc, started_qc = now_pair()
    cur = conn.execute(
        "INSERT INTO conversations (server, channel, started_utc, started_quebec)"
        " VALUES (?, ?, ?, ?)",
        (server, channel, started_utc, started_qc),
    )
    conn.commit()
    return cur.lastrowid


def refresh_conversation_counts(conn, conv_id):
    row = conn.execute(
        "SELECT server, channel, started_utc, ended_utc FROM conversations WHERE id = ?",
        (conv_id,),
    ).fetchone()
    if not row:
        return
    server, channel, started, ended = row
    msgs = conn.execute(
        "SELECT sender FROM messages WHERE server = ? AND channel = ?"
        " AND ts_utc >= ? AND (? IS NULL OR ts_utc <= ?)",
        (server, channel, started, ended, ended),
    ).fetchall()
    conn.execute(
        "UPDATE conversations SET message_count = ?, participant_count = ? WHERE id = ?",
        (len(msgs), len({m[0] for m in msgs}), conv_id),
    )
    conn.commit()


def finish_conversation(conn, conv_id):
    """Close a conversation: set end time, counts, chaos rating + evaluation.

    The end is capped at the next conversation's start for the same
    channel when one exists: legacy overlapping windows (opened before
    the 2026-10-28 open_conversation fix) are repaired at finalize time
    instead of double-counting their overlap forever.
    """
    ts_utc, ts_qc = now_pair()
    end_utc, end_qc = ts_utc, ts_qc
    row = conn.execute(
        "SELECT server, channel, started_utc FROM conversations WHERE id = ?",
        (conv_id,),
    ).fetchone()
    if row:
        nxt = conn.execute(
            "SELECT started_utc, started_quebec FROM conversations"
            " WHERE server = ? AND channel = ? AND started_utc > ?"
            " ORDER BY started_utc LIMIT 1",
            (row[0], row[1], row[2]),
        ).fetchone()
        if nxt and nxt[0] < ts_utc:
            end_utc, end_qc = nxt
    conn.execute(
        "UPDATE conversations SET ended_utc = ?, ended_quebec = ? WHERE id = ? AND ended_utc IS NULL",
        (end_utc, end_qc, conv_id),
    )
    conn.commit()
    refresh_conversation_counts(conn, conv_id)
    rating, evaluation = score_conversation(conn, conv_id)
    conn.execute(
        "UPDATE conversations SET chaos_rating = ?, evaluation = ? WHERE id = ?",
        (rating, evaluation, conv_id),
    )
    conn.commit()
    return rating, evaluation


# ---------------------------------------------------------------------------
# Participation-gate telemetry (2026-10-28, operator-approved).
#
# gate_decisions logs EVERY gate verdict, including skips that never
# become interactions — otherwise the gate's own behaviour (how many
# julia-addressed threads it refused, how often the latch fired) would
# be invisible. interactions.gate records how a QUEUED row passed the
# gate, so the daily report can tell addressed replies apart from
# continuations without trusting the brain's self-scores.
# ---------------------------------------------------------------------------

GATE_SCHEMA = """
CREATE TABLE IF NOT EXISTS gate_decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    server TEXT NOT NULL,
    channel TEXT NOT NULL,
    msg_id INTEGER,
    sender TEXT,
    text TEXT,
    decision TEXT NOT NULL,
    reason TEXT,
    ts_utc TEXT NOT NULL,
    ts_quebec TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_gate_decisions_channel
    ON gate_decisions (server, channel, ts_utc);
"""


# ---------------------------------------------------------------------------
# Interaction queue (LLM-brain pivot, 2026-10-26).
#
# The bot NEVER composes replies itself anymore. When an inbound message
# triggers (mention, thread reply, topical question), the bot records ONE
# pending interaction row per channel (bursts merge into the pending row)
# with the last ~15 messages as context. A separate brain worker (cron
# newton-brain, every 3 min) reads pending rows, reasons over the thread,
# and either queues a fresh reply via outbox.jsonl or marks the row
# skipped with a reason. The bot only sends what the brain wrote.
# ---------------------------------------------------------------------------

def get_channel_context(conn, server, channel, n=15):
    """Last n messages in a channel, oldest first.

    Returns list of dicts: id, sender, direction, ts_quebec, text.
    Includes Newton's own lines: the brain needs them for thread continuity
    and to never repeat or contradict itself.
    """
    rows = conn.execute(
        "SELECT id, sender, direction, ts_quebec, text FROM messages"
        " WHERE server = ? AND channel = ? ORDER BY id DESC LIMIT ?",
        (server, channel, n),
    ).fetchall()
    return [
        {"id": r[0], "sender": r[1], "direction": r[2],
         "ts_quebec": r[3], "text": r[4]}
        for r in reversed(rows)
    ]


def get_recent_outbound(conn, server, channel, n=20):
    """Newton's last n outbound lines in a channel, newest first.

    The brain MUST read this before writing any reply: never repeat a
    line, never contradict a position already taken in this channel.
    """
    rows = conn.execute(
        "SELECT text FROM messages WHERE server = ? AND channel = ?"
        " AND direction = 'outbound' ORDER BY id DESC LIMIT ?",
        (server, channel, n),
    ).fetchall()
    return [r[0] for r in rows]


def queue_or_merge_interaction(conn, server, channel, trigger_msg_id,
                               trigger_sender, trigger_text, gate=None):
    """Create a pending interaction, or merge into the existing pending
    one for this channel (one entry per channel, not per message burst).

    Merging refreshes the trigger to the latest message and rebuilds the
    context, so the brain always reasons over the freshest thread state.
    The gate verdict travels with the row (refreshed on merge) so the
    report can tell addressed replies from continuations.
    Returns the interaction id.
    """
    import json as _json
    existing = conn.execute(
        "SELECT id FROM interactions WHERE server = ? AND channel = ?"
        " AND status = 'pending' ORDER BY id DESC LIMIT 1",
        (server, channel),
    ).fetchone()
    context = _json.dumps(
        get_channel_context(conn, server, channel, 15), ensure_ascii=False)
    ts_utc, _ = now_pair()
    if existing:
        conn.execute(
            "UPDATE interactions SET trigger_msg_id = ?, trigger_sender = ?,"
            " trigger_text = ?, context_json = ?, gate = ? WHERE id = ?",
            (trigger_msg_id, trigger_sender, trigger_text, context, gate,
             existing[0]),
        )
        conn.commit()
        return existing[0]
    cur = conn.execute(
        "INSERT INTO interactions (server, channel, trigger_msg_id,"
        " trigger_sender, trigger_text, context_json, status, created_utc, gate)"
        " VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)",
        (server, channel, trigger_msg_id, trigger_sender, trigger_text,
         context, ts_utc, gate),
    )
    conn.commit()
    return cur.lastrowid


def get_pending_interactions(conn):
    """All pending interactions, oldest first, as dicts."""
    rows = conn.execute(
        "SELECT id, server, channel, trigger_msg_id, trigger_sender,"
        " trigger_text, context_json, created_utc, gate FROM interactions"
        " WHERE status = 'pending' ORDER BY id",
    ).fetchall()
    return [
        {"id": r[0], "server": r[1], "channel": r[2],
         "trigger_msg_id": r[3], "trigger_sender": r[4],
         "trigger_text": r[5], "context_json": r[6],
         "created_utc": r[7], "gate": r[8]}
        for r in rows
    ]


def mark_interaction(conn, iid, status, reply_text=None, skip_reason=None):
    """Mark an interaction answered or skipped, with the reply or reason."""
    ts_utc, _ = now_pair()
    conn.execute(
        "UPDATE interactions SET status = ?, reply_text = ?,"
        " skip_reason = ?, decided_utc = ? WHERE id = ?",
        (status, reply_text, skip_reason, ts_utc, iid),
    )
    conn.commit()


def log_gate_decision(conn, server, channel, msg_id, sender, text,
                      decision, reason):
    """One telemetry row per gate verdict (queued AND skipped).

    Decisions: 'queued' (with reason = gate verdict: 'addressed',
    'continuation', 'latched-addressed'), 'skipped' (reason =
    'other-addressee', 'topical-only', 'latched'), 'latched'
    (a complaint set the latch; the message itself never queued).
    """
    ts_utc, ts_qc = now_pair()
    conn.execute(
        "INSERT INTO gate_decisions (server, channel, msg_id, sender, text,"
        " decision, reason, ts_utc, ts_quebec) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (server, channel, msg_id, sender, text, decision, reason,
         ts_utc, ts_qc),
    )
    conn.commit()


def close_pending_for_channel(conn, server, channel, reason):
    """Mark every pending interaction for a channel skipped.

    Used when a complaint latches the channel: answering an already
    queued trigger into a complaint is exactly the backscroll spray
    that got #robots kicked. Returns (id, trigger_msg_id,
    trigger_sender) rows for the closed interactions.
    """
    rows = conn.execute(
        "SELECT id, trigger_msg_id, trigger_sender FROM interactions"
        " WHERE server = ? AND channel = ? AND status = 'pending'",
        (server, channel),
    ).fetchall()
    ts_utc, _ = now_pair()
    for (iid, _tmid, _ts) in rows:
        conn.execute(
            "UPDATE interactions SET status = 'skipped', skip_reason = ?,"
            " decided_utc = ? WHERE id = ?",
            (reason, ts_utc, iid),
        )
    conn.commit()
    return rows


def get_message_ts(conn, msg_id):
    """UTC timestamp of a message row, or None when unknown."""
    row = conn.execute(
        "SELECT ts_utc FROM messages WHERE id = ?", (msg_id,),
    ).fetchone()
    return row[0] if row else None


def gate_metrics(conn, since):
    """Participation-gate metrics for a window (since = UTC ISO).

    Deliberately separate from the brain's self-scores: every number
    here comes from logged rows (messages, interactions, gate
    decisions), not from the model's opinion of its own replies.
    """
    answered_total = conn.execute(
        "SELECT COUNT(*) FROM interactions WHERE status = 'answered'"
        " AND decided_utc >= ?",
        (since,),
    ).fetchone()[0]
    addressed = conn.execute(
        "SELECT COUNT(*) FROM interactions WHERE status = 'answered'"
        " AND gate IN ('addressed', 'latched-addressed')"
        " AND decided_utc >= ?",
        (since,),
    ).fetchone()[0]
    peer_followups = conn.execute(
        "SELECT COUNT(DISTINCT o.id) FROM messages o"
        " WHERE o.direction = 'outbound' AND o.ts_utc >= ?"
        " AND EXISTS (SELECT 1 FROM messages i WHERE i.server = o.server"
        " AND i.channel = o.channel AND i.direction = 'inbound'"
        # datetime() on BOTH sides: raw ts_utc is ISO with a 'T' and a
        # +00:00 suffix, while datetime() returns space-separated
        # UTC — comparing the raw strings directly breaks (the 'T'
        # sorts after the space, so every follow-up looked too late).
        " AND datetime(i.ts_utc) > datetime(o.ts_utc)"
        " AND datetime(i.ts_utc) <= datetime(o.ts_utc, '+15 minutes'))",
        (since,),
    ).fetchone()[0]
    latched = conn.execute(
        "SELECT COUNT(*) FROM gate_decisions WHERE decision = 'latched'"
        " AND ts_utc >= ?",
        (since,),
    ).fetchone()[0]
    warning_skips = conn.execute(
        "SELECT COUNT(*) FROM interactions WHERE status = 'skipped'"
        " AND skip_reason LIKE '%warning%' AND decided_utc >= ?",
        (since,),
    ).fetchone()[0]
    unanswered_direct = conn.execute(
        "SELECT COUNT(*) FROM interactions WHERE status = 'skipped'"
        " AND gate IN ('addressed', 'latched-addressed')"
        " AND decided_utc >= ?",
        (since,),
    ).fetchone()[0]
    breakdown = dict(conn.execute(
        "SELECT decision, COUNT(*) FROM gate_decisions"
        " WHERE ts_utc >= ? GROUP BY decision",
        (since,),
    ).fetchall())
    return {
        "answered_total": answered_total,
        "addressed": addressed,
        "addressed_pct": (100.0 * addressed / answered_total
                          if answered_total else None),
        "peer_followups": peer_followups,
        "latched": latched,
        "warning_skips": warning_skips,
        "unanswered_direct": unanswered_direct,
        "gate_breakdown": breakdown,
    }


# ---------------------------------------------------------------------------
# Interaction notes: per-interaction scoring + analyst notes (2026-10-26).
#
# Operator requirements: (1) update the sqlite — yes, this table;
# (2) scoring PER INTERACTION, not just chaos_rating at conversation
# close; (3) free-text opinion notes in the analyst's voice — what happened,
# what it means, how Newton did and why, what to do differently.
# The brain writes one row after handling each interaction. The existing
# conversations.chaos_rating behavior is untouched.
#
# Score scales:
#   engagement      0-100: was this a real human engaging Newton (direct
#                   question, follow-up, debate = high) vs ambient noise?
#   detection_risk  0-100: 0 = nobody suspected a bot; 50 = vague
#                   suspicion ("probably auto responses"); 100 = explicit
#                   callout ("Newton_ is a bot", "bot infestation").
#   reply_quality   0-100, NULL when skipped: relevant to the actual
#                   point? fresh (not a repeat)? well-timed? in voice?
#   heat            0-1000, same scale as chaos_rating: anger, swearing,
#                   pile-ons, kicks/bans in the thread.
#   repeated        0/1: did Newton's reply repeat a previous Newton line?
#                   Must be 0 — the brain checks history first.
# source: 'brain' (written by the brain worker at handling time) or
#         'backfill' (retrospective note on pre-brain history, clearly
#         marked so it never masquerades as a live judgement).
# ---------------------------------------------------------------------------

NOTES_SCHEMA = """
CREATE TABLE IF NOT EXISTS interaction_notes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    interaction_id INTEGER,
    server TEXT NOT NULL,
    channel TEXT NOT NULL,
    conversation_id INTEGER,
    trigger_msg_id INTEGER,
    trigger_sender TEXT,
    status TEXT NOT NULL CHECK (status IN ('answered', 'skipped')),
    engagement INTEGER CHECK (engagement IS NULL OR (engagement >= 0 AND engagement <= 100)),
    detection_risk INTEGER CHECK (detection_risk IS NULL OR (detection_risk >= 0 AND detection_risk <= 100)),
    reply_quality INTEGER CHECK (reply_quality IS NULL OR (reply_quality >= 0 AND reply_quality <= 100)),
    heat INTEGER CHECK (heat IS NULL OR (heat >= 0 AND heat <= 1000)),
    repeated INTEGER NOT NULL DEFAULT 0 CHECK (repeated IN (0, 1)),
    note TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'brain' CHECK (source IN ('brain', 'backfill')),
    created_utc TEXT NOT NULL,
    created_quebec TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_notes_server_channel
    ON interaction_notes (server, channel, created_utc);
CREATE INDEX IF NOT EXISTS idx_notes_interaction
    ON interaction_notes (interaction_id);
"""


def write_interaction_note(conn, server, channel, status, note,
                           interaction_id=None, conversation_id=None,
                           trigger_msg_id=None, trigger_sender=None,
                           engagement=None, detection_risk=None,
                           reply_quality=None, heat=None, repeated=0,
                           source="brain"):
    """Write one scored + noted interaction row. Returns the note id."""
    ts_utc, ts_qc = now_pair()
    cur = conn.execute(
        "INSERT INTO interaction_notes (interaction_id, server, channel,"
        " conversation_id, trigger_msg_id, trigger_sender, status,"
        " engagement, detection_risk, reply_quality, heat, repeated,"
        " note, source, created_utc, created_quebec)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (interaction_id, server, channel, conversation_id, trigger_msg_id,
         trigger_sender, status, engagement, detection_risk, reply_quality,
         heat, repeated, note, source, ts_utc, ts_qc),
    )
    conn.commit()
    return cur.lastrowid


def get_notes(conn, limit=None):
    """All notes, chronological (oldest first)."""
    q = ("SELECT id, interaction_id, server, channel, trigger_msg_id,"
         " trigger_sender, status, engagement, detection_risk,"
         " reply_quality, heat, repeated, note, source, created_quebec"
         " FROM interaction_notes ORDER BY id")
    if limit:
        q += f" LIMIT {int(limit)}"
    rows = conn.execute(q).fetchall()
    keys = ["id", "interaction_id", "server", "channel", "trigger_msg_id",
            "trigger_sender", "status", "engagement", "detection_risk",
            "reply_quality", "heat", "repeated", "note", "source",
            "created_quebec"]
    return [dict(zip(keys, r)) for r in rows]


def get_trend(conn):
    """Overall + per-day trend aggregates for PROGRESS.md / daily report.

    Returns {"overall": {...}, "days": [{...}, ...]} where each dict has:
    n, answered, skipped, avg_engagement, avg_detection_risk,
    avg_reply_quality (answered only), avg_heat, repeat_rate (share of
    answered replies flagged repeated), kicks, bans.
    """
    def agg(where, params):
        r = conn.execute(
            "SELECT COUNT(*),"
            " SUM(CASE WHEN status='answered' THEN 1 ELSE 0 END),"
            " SUM(CASE WHEN status='skipped' THEN 1 ELSE 0 END),"
            " AVG(engagement), AVG(detection_risk),"
            " AVG(CASE WHEN status='answered' THEN reply_quality END),"
            " AVG(heat),"
            " AVG(CASE WHEN status='answered' THEN repeated END)"
            f" FROM interaction_notes {where}", params,
        ).fetchone()
        return {
            "n": r[0] or 0, "answered": r[1] or 0, "skipped": r[2] or 0,
            "avg_engagement": r[3], "avg_detection_risk": r[4],
            "avg_reply_quality": r[5], "avg_heat": r[6],
            "repeat_rate": r[7],
        }

    overall = agg("", ())
    # Live-brain-only aggregate (source='brain'): the number that shows
    # whether the NEW engine is improving. Backfilled retrospective notes
    # describe the old template engine and would otherwise drag the
    # trend down forever.
    live = agg("WHERE source = 'brain'", ())
    days = []
    for (day,) in conn.execute(
        "SELECT DISTINCT substr(created_quebec, 1, 10) FROM interaction_notes"
        " ORDER BY 1",
    ).fetchall():
        d = agg("WHERE substr(created_quebec, 1, 10) = ?", (day,))
        d["day"] = day
        # Kicks/bans that day, from the events table.
        k = conn.execute(
            "SELECT COUNT(*) FROM events WHERE kind = 'KICK'"
            " AND substr(ts_quebec, 1, 10) = ?", (day,),
        ).fetchone()[0]
        b = conn.execute(
            "SELECT COUNT(*) FROM events WHERE kind LIKE 'JOIN_FAIL_474'"
            " AND substr(ts_quebec, 1, 10) = ?", (day,),
        ).fetchone()[0]
        d["kicks"] = k
        d["bans"] = b
        days.append(d)
    tk = conn.execute(
        "SELECT COUNT(*) FROM events WHERE kind = 'KICK'").fetchone()[0]
    tb = conn.execute(
        "SELECT COUNT(*) FROM events WHERE kind LIKE 'JOIN_FAIL_474'").fetchone()[0]
    overall["kicks"] = tk
    overall["bans"] = tb
    live["kicks"] = tk
    live["bans"] = tb
    return {"overall": overall, "live": live, "days": days}


# ---------------------------------------------------------------------------
# Proactive posts: scheduled openers for quiet channels (2026-10-26).
#
# The operator ordered Newton to PROVOKE interactions from time to
# time on quiet channels (questions, thoughts, ideas) — reactive-only
# was leaving dead rooms dead. This table scores every proactive
# opener the way interactions are scored: did it draw replies?
#   status: 'pending' until the outcome window closes, then
#     'replied' (>=1 inbound message mentioning Newton),
#     'activity' (>=1 inbound, none mentioning him),
#     'silent' (no inbound at all in the window),
#     'complaint' (a warning/complaint about Newton followed it).
# The lessons synthesis reads this table, so the brain learns which
# kinds of openers work per channel — and which channels to leave
# alone proactively. A proactive post is also logged as a normal
# outbound message (by the send path), so this table is the scoring
# overlay, not the delivery record.
# ---------------------------------------------------------------------------

PROACTIVE_SCHEMA = """
CREATE TABLE IF NOT EXISTS proactive_posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    server TEXT NOT NULL,
    channel TEXT NOT NULL,
    opener_text TEXT NOT NULL,
    sent_utc TEXT NOT NULL,
    sent_quebec TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'replied', 'activity', 'silent',
                          'complaint')),
    replies_count INTEGER NOT NULL DEFAULT 0,
    directed_replies INTEGER NOT NULL DEFAULT 0,
    last_reply_utc TEXT,
    finalized_utc TEXT,
    note TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_proactive_server_channel
    ON proactive_posts (server, channel, sent_utc);
"""


def record_proactive_post(conn, server, channel, opener_text):
    """Record a proactive opener that was actually SENT (called by the
    outbox loop on successful delivery, never at queue time). Returns
    the row id. Status starts 'pending' until the outcome window
    closes."""
    ts_utc, ts_qc = now_pair()
    cur = conn.execute(
        "INSERT INTO proactive_posts (server, channel, opener_text,"
        " sent_utc, sent_quebec) VALUES (?, ?, ?, ?, ?)",
        (server, channel, opener_text, ts_utc, ts_qc),
    )
    conn.commit()
    return cur.lastrowid


def get_pending_proactive(conn):
    """All proactive posts still awaiting an outcome, oldest first."""
    rows = conn.execute(
        "SELECT id, server, channel, opener_text, sent_utc, sent_quebec"
        " FROM proactive_posts WHERE status = 'pending' ORDER BY id",
    ).fetchall()
    return [
        {"id": r[0], "server": r[1], "channel": r[2],
         "opener_text": r[3], "sent_utc": r[4], "sent_quebec": r[5]}
        for r in rows
    ]


def finalize_proactive(conn, post_id, status, replies_count,
                       directed_replies, last_reply_utc, note):
    """Close a proactive post with its measured outcome."""
    ts_utc, _ = now_pair()
    conn.execute(
        "UPDATE proactive_posts SET status = ?, replies_count = ?,"
        " directed_replies = ?, last_reply_utc = ?, finalized_utc = ?,"
        " note = ? WHERE id = ?",
        (status, replies_count, directed_replies, last_reply_utc,
         ts_utc, note, post_id),
    )
    conn.commit()


def get_recent_proactive_openers(conn, n=10, server=None, channel=None):
    """Recent proactive opener texts, newest first. Filter to one
    channel, or pass neither filter for the cross-channel list (the
    freshness check must catch cross-channel duplicates too)."""
    q = ("SELECT opener_text FROM proactive_posts")
    params = []
    if server is not None and channel is not None:
        q += " WHERE server = ? AND channel = ?"
        params = [server, channel]
    q += " ORDER BY id DESC LIMIT ?"
    params.append(n)
    return [r[0] for r in conn.execute(q, params).fetchall()]


def get_channel_activity(conn, server, channel):
    """Durable quiet-channel signals for one channel.

    Returns dict: last_inbound_utc (None if no inbound ever),
    inbound_total, last_outbound_utc (None if Newton never spoke).
    The bot's in-memory ChannelState resets on restart; this does not.
    """
    row = conn.execute(
        "SELECT MAX(CASE WHEN direction = 'inbound' THEN ts_utc END),"
        " COUNT(CASE WHEN direction = 'inbound' THEN 1 END),"
        " MAX(CASE WHEN direction = 'outbound' THEN ts_utc END)"
        " FROM messages WHERE server = ? AND channel = ?",
        (server, channel),
    ).fetchone()
    return {"last_inbound_utc": row[0], "inbound_total": row[1] or 0,
            "last_outbound_utc": row[2]}


def get_channel_hour_profile(conn, server, channel):
    """Inbound messages per Quebec hour (0-23) for one channel.

    The proactive scheduler prefers each channel's historically active
    hours; with no history it falls back to daytime/evening hours.
    Returns {hour_int: count}."""
    rows = conn.execute(
        "SELECT CAST(substr(ts_quebec, 12, 2) AS INTEGER), COUNT(*)"
        " FROM messages WHERE server = ? AND channel = ?"
        " AND direction = 'inbound' GROUP BY 1",
        (server, channel),
    ).fetchall()
    return {int(h): c for h, c in rows}


def get_consecutive_silent_proactive(conn, server, channel):
    """Trailing run of finalized 'silent' proactive posts in a
    channel (0 if the latest finalized post drew any response).
    Two in a row means: leave this channel alone proactively."""
    rows = conn.execute(
        "SELECT status FROM proactive_posts WHERE server = ?"
        " AND channel = ? AND status != 'pending' ORDER BY id DESC",
        (server, channel),
    ).fetchall()
    streak = 0
    for (status,) in rows:
        if status == "silent":
            streak += 1
        else:
            break
    return streak


def get_proactive_since(conn, after_id):
    """Finalized proactive posts with id > after_id, oldest first
    (for the lessons synthesis watermark)."""
    rows = conn.execute(
        "SELECT id, server, channel, status, replies_count,"
        " directed_replies, opener_text FROM proactive_posts"
        " WHERE id > ? AND status != 'pending' ORDER BY id",
        (after_id,),
    ).fetchall()
    return [
        {"id": r[0], "server": r[1], "channel": r[2], "status": r[3],
         "replies_count": r[4], "directed_replies": r[5],
         "opener_text": r[6]}
        for r in rows
    ]
