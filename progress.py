#!/usr/bin/env python3
"""Regenerate Newton's human-readable progress document (PROGRESS.md).

Reads interaction_notes (+ events for kicks/bans) from newton.db and
writes PROGRESS.md: trends first (detection risk, reply quality,
engagement, repeat rate, kicks/bans — overall + per day), then a
chronological list of scored interactions with channel, who, what
happened, scores, and the analyst note. Plain language, short entries:
a story of progress, not a SQL dump.

Run by: the brain worker after closing a batch of interactions, and the
09:00 daily report (daily_report.py). Safe to run any time; it only
reads the DB and rewrites PROGRESS.md.

Usage: python3 progress.py [--trend]   (--trend prints only the trend
section to stdout, for embedding in the daily report.)
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import newton_db

BASE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE, "newton.db")
OUT_PATH = os.path.join(BASE, "PROGRESS.md")


def _fmt_avg(v):
    return "—" if v is None else f"{v:.0f}"


def _verdict(trend):
    """Plain-language verdict lines.

    Keys off the LIVE-brain aggregate when live notes exist (that is the
    number that shows whether the new engine is improving); the overall
    aggregate includes retrospective notes about the old template engine
    and is shown separately, never hidden.
    """
    live = trend.get("live") or {}
    o = live if live.get("n") else trend["overall"]
    lines = []
    if trend["overall"]["n"] == 0:
        return ["No scored interactions yet — the brain has not closed any. "
                "Scores and notes will appear here as interactions are handled."]
    if not live.get("n"):
        # All scores so far are retrospective notes about the OLD template
        # engine. Say so explicitly, or the verdicts read as if the new
        # brain were already failing.
        lines.append("No live-brain scores yet — every score below describes "
                     "the OLD template engine (retrospective notes), not the "
                     "new brain. Judge the brain once it has scored real "
                     "interactions.")
    if o["avg_detection_risk"] is not None:
        d = o["avg_detection_risk"]
        if d < 25:
            lines.append(f"Detection risk is low (avg {d:.0f}/100) — rooms "
                         "mostly don't suspect him.")
        elif d < 60:
            lines.append(f"Detection risk is middling (avg {d:.0f}/100) — "
                         "some rooms are suspicious; watch the notes.")
        else:
            lines.append(f"Detection risk is HIGH (avg {d:.0f}/100) — rooms "
                         "are clocking him; something in his replies is off.")
    if o["avg_reply_quality"] is not None:
        q = o["avg_reply_quality"]
        if q >= 70:
            lines.append(f"Reply quality is strong (avg {q:.0f}/100 on "
                         "answered interactions).")
        elif q >= 45:
            lines.append(f"Reply quality is mixed (avg {q:.0f}/100) — "
                         "read the low-scored notes for what to fix.")
        else:
            lines.append(f"Reply quality is weak (avg {q:.0f}/100) — "
                         "replies are missing the point or sounding canned.")
    if o["repeat_rate"] is not None:
        rr = o["repeat_rate"] * 100
        if rr == 0:
            lines.append("Repeat rate is 0% — no reply has repeated a "
                         "previous Newton line since scoring began.")
        else:
            lines.append(f"Repeat rate is {rr:.0f}% — repeats are back; "
                         "this is the failure mode that got him called out.")
    if o["kicks"] or o["bans"]:
        lines.append(f"Kicks: {o['kicks']}, channel bans: {o['bans']} "
                     "(all time, from the events log).")
    else:
        lines.append("No kicks or bans recorded since scoring began.")
    return lines


def render_trend(trend):
    o = trend["overall"]
    live = trend.get("live") or {}
    out = ["## Trend summary\n"]
    out.append(f"Scored interactions: {o['n']} "
               f"({o['answered']} answered, {o['skipped']} skipped).")
    out.append(f"Average detection risk: {_fmt_avg(o['avg_detection_risk'])}/100 "
               f"(lower is better).")
    out.append(f"Average reply quality: {_fmt_avg(o['avg_reply_quality'])}/100 "
               "(answered only, higher is better).")
    out.append(f"Average engagement: {_fmt_avg(o['avg_engagement'])}/100.")
    rr = "—" if o["repeat_rate"] is None else f"{o['repeat_rate'] * 100:.0f}%"
    out.append(f"Repeat rate: {rr}.")
    out.append(f"Kicks: {o['kicks']}, channel bans: {o['bans']} (all time).")
    if live.get("n"):
        lr = "—" if live["repeat_rate"] is None else f"{live['repeat_rate'] * 100:.0f}%"
        out.append(f"Live brain only ({live['n']} scored): detection risk "
                   f"{_fmt_avg(live['avg_detection_risk'])}/100, reply quality "
                   f"{_fmt_avg(live['avg_reply_quality'])}/100, engagement "
                   f"{_fmt_avg(live['avg_engagement'])}/100, repeat rate {lr}.")
        out.append("Overall numbers above include retrospective notes about "
                   "the old template engine; the live-brain line is the one "
                   "that shows current progress.")
    else:
        out.append("All scores so far are retrospective notes about the old "
                   "template engine; live-brain scores will appear here as "
                   "the brain closes interactions.")
    out.append("")
    for v in _verdict(trend):
        out.append(f"- {v}")
    out.append("")
    if trend["days"]:
        out.append("### Per day\n")
        out.append("| Day | Handled | Answered | Avg detection | "
                   "Avg reply quality | Avg engagement | Repeats | Kicks | Bans |")
        out.append("|-----|---------|----------|---------------|-------------------|"
                   "----------------|---------|-------|------|")
        for d in trend["days"]:
            rr = "—" if d["repeat_rate"] is None else f"{d['repeat_rate'] * 100:.0f}%"
            out.append(f"| {d['day']} | {d['n']} | {d['answered']} | "
                       f"{_fmt_avg(d['avg_detection_risk'])} | "
                       f"{_fmt_avg(d['avg_reply_quality'])} | "
                       f"{_fmt_avg(d['avg_engagement'])} | {rr} | "
                       f"{d['kicks']} | {d['bans']} |")
        out.append("")
    return "\n".join(out)


def build_markdown():
    conn = newton_db.connect(DB_PATH)
    trend = newton_db.get_trend(conn)
    notes = newton_db.get_notes(conn)
    # Resolve trigger text (messages table, by trigger_msg_id) and reply
    # text (interactions table, by interaction_id) for each note.
    trig_ids = [n["trigger_msg_id"] for n in notes
                if n["trigger_msg_id"] is not None]
    texts = {}
    if trig_ids:
        q = ("SELECT id, text FROM messages WHERE id IN (%s)" %
             ",".join("?" * len(trig_ids)))
        texts = dict(conn.execute(q, trig_ids).fetchall())
    int_ids = [n["interaction_id"] for n in notes
               if n["interaction_id"] is not None]
    replies = {}
    if int_ids:
        q = ("SELECT id, reply_text FROM interactions WHERE id IN (%s)" %
             ",".join("?" * len(int_ids)))
        replies = dict(conn.execute(q, int_ids).fetchall())
    # Backfilled notes have no interaction row: resolve Newton's actual
    # reply as the first outbound message in that channel after the
    # trigger message.
    for n in notes:
        if n["interaction_id"] is None and n["status"] == "answered" \
                and n["trigger_msg_id"] is not None:
            row = conn.execute(
                "SELECT text FROM messages WHERE server = ? AND channel = ?"
                " AND direction = 'outbound' AND id > ? ORDER BY id LIMIT 1",
                (n["server"], n["channel"], n["trigger_msg_id"])).fetchone()
            if row:
                replies[n["id"]] = row[0]

    _, ts_qc = newton_db.now_pair()
    lines = ["# Newton IRC — Progress\n",
             "How Newton is doing on IRC, in plain language. Trends first, "
             "then every scored interaction in chronological order. "
             "Regenerated from the scored-interactions log (newton.db, "
             "interaction_notes table) — never edited by hand.\n",
             f"Last updated: {ts_qc}\n"]
    lines.append(render_trend(trend))
    lines.append("## Scored interactions (chronological)\n")
    if not notes:
        lines.append("None yet.\n")
    for n in notes:
        trig = texts.get(n["trigger_msg_id"] or -1, "")
        # Live notes: reply keyed by interaction id. Backfilled notes:
        # reply keyed by note id (resolved from messages above).
        reply = replies.get(n["interaction_id"] or -1)
        if reply is None:
            reply = replies.get(n["id"])
        when = n["created_quebec"][:16]
        src = " (retrospective)" if n["source"] == "backfill" else ""
        lines.append(f"### {when} — {n['server']} {n['channel']} — "
                     f"{n['trigger_sender'] or 'someone'}{src}")
        if trig:
            lines.append(f"> Trigger: \"{trig[:200]}\"")
        if n["status"] == "answered":
            if reply:
                lines.append(f"> Newton replied: \"{reply[:300]}\"")
            else:
                lines.append("> Newton replied (text not linked — see note).")
        else:
            lines.append("> Newton stayed silent (skipped).")
        scores = (f"Scores: engagement {n['engagement'] if n['engagement'] is not None else '—'}/100 · "
                  f"detection risk {n['detection_risk'] if n['detection_risk'] is not None else '—'}/100 · "
                  f"reply quality {n['reply_quality'] if n['reply_quality'] is not None else '—'}/100 · "
                  f"heat {n['heat'] if n['heat'] is not None else '—'}/1000")
        if n["repeated"]:
            scores += " · ⚠ REPEATED a previous line"
        lines.append(scores)
        lines.append(n["note"])
        lines.append("")
    return "\n".join(lines)


def main():
    trend_only = "--trend" in sys.argv
    if trend_only:
        conn = newton_db.connect(DB_PATH)
        print(render_trend(newton_db.get_trend(conn)))
        return
    text = build_markdown()
    tmp = OUT_PATH + ".tmp"
    with open(tmp, "w") as f:
        f.write(text)
    os.replace(tmp, OUT_PATH)
    print(f"wrote {OUT_PATH}")


if __name__ == "__main__":
    main()
