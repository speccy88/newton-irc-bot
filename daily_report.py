#!/usr/bin/env python3
"""Newton IRC — daily havoc report + Google Drive backup.

Run daily at 09:00 America/Toronto (cron). Does three things:
  1. Prints a summary of the last 24h: messages sent/received, conversations
     finished with chaos ratings + evaluation excerpts, per server/channel.
     (The cron job delivers this text to the operator as the daily report.)
  2. Regenerates PROGRESS.md (progress.py) and embeds its trend summary
     (detection risk, reply quality, engagement, repeat rate, kicks/bans)
     in the report, so the operator sees progress trends every morning.
  3. Backs up newton.db to Google Drive, REUSING THE SAME FILE every day:
     first run uploads to the Charlie Drive MEM folder and records the file
     ID in drive_backup.json; later runs overwrite that same file with
     `drive files update --params '{"fileId":"<id>"}' --upload <path>`.
     Never creates duplicates.

Stale conversations (no activity for 30+ min) are finalized (scored) before
the summary is built, so the report includes their chaos ratings.
"""
import json
import os
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import newton_db

BASE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE, "newton.db")
STATE_PATH = os.path.join(BASE, "drive_backup.json")
MEM_FOLDER_ID = "1ZdiA2Y1rv8Q-jhUmTrdFfx66ru49sg5w"
BACKUP_NAME = "newton-irc-log.sqlite"


def finalize_stale(conn, idle_s=1800):
    """Finish conversations with no message activity for idle_s seconds."""
    now = datetime.now(timezone.utc)
    count = 0
    for row in conn.execute(
        "SELECT id, server, channel FROM conversations WHERE ended_utc IS NULL"
    ).fetchall():
        conv_id, server, channel = row
        last = conn.execute(
            "SELECT MAX(ts_utc) FROM messages WHERE server = ? AND channel = ?",
            (server, channel),
        ).fetchone()[0]
        if last is None:
            # Empty conversation (joined, nobody spoke): use its start time.
            last = conn.execute(
                "SELECT started_utc FROM conversations WHERE id = ?", (conv_id,)
            ).fetchone()[0]
        if (now - datetime.fromisoformat(last)).total_seconds() >= idle_s:
            rating, _ev = newton_db.finish_conversation(conn, conv_id)
            count += 1
    return count


def build_report(conn):
    since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    lines = []
    lines.append("NEWTON IRC — DAILY HAVOC REPORT (last 24h)")
    lines.append(f"Generated: {datetime.now(timezone.utc).isoformat()} UTC / "
                 f"{datetime.now(newton_db.QUEBEC).isoformat()} Quebec time")
    lines.append("")
    total_out = conn.execute(
        "SELECT COUNT(*) FROM messages WHERE direction = 'outbound' AND ts_utc >= ?",
        (since,)).fetchone()[0]
    total_in = conn.execute(
        "SELECT COUNT(*) FROM messages WHERE direction = 'inbound' AND ts_utc >= ?",
        (since,)).fetchone()[0]
    lines.append(f"Messages sent by Newton: {total_out}")
    lines.append(f"Messages received: {total_in}")
    lines.append("")
    lines.append("Per server/channel (last 24h):")
    for server, channel, d, n in conn.execute(
        "SELECT server, channel, direction, COUNT(*) FROM messages WHERE ts_utc >= ?"
        " GROUP BY server, channel, direction ORDER BY server, channel, direction",
        (since,)).fetchall():
        lines.append(f"  {server} {channel} {d}: {n}")
    lines.append("")
    lines.append("PARTICIPATION GATE (last 24h, from logged rows — not self-scores):")
    try:
        gm = newton_db.gate_metrics(conn, since)
        pct = (f"{gm['addressed_pct']:.0f}%" if gm["addressed_pct"] is not None
               else "n/a (no answered interactions)")
        lines.append(f"  Answered interactions: {gm['answered_total']},"
                     f" of which directly addressed: {gm['addressed']} ({pct})")
        lines.append(f"  Newton replies that drew a peer follow-up within 15 min:"
                     f" {gm['peer_followups']}")
        lines.append(f"  Complaint latches set: {gm['latched']},"
                     f" warning skips: {gm['warning_skips']}")
        lines.append(f"  Direct questions left unanswered: {gm['unanswered_direct']}")
        bd = gm["gate_breakdown"]
        if bd:
            lines.append("  Gate verdicts: " + ", ".join(
                f"{k}={v}" for k, v in sorted(bd.items())))
        else:
            lines.append("  Gate verdicts: (none logged — gate not yet deployed)")
    except Exception as e:
        lines.append(f"  (gate metrics unavailable: {e})")
    lines.append("")
    lines.append("Conversations finished (last 24h, with chaos rating 0-1000):")
    rows = conn.execute(
        "SELECT server, channel, started_quebec, ended_quebec, message_count,"
        " participant_count, chaos_rating, evaluation FROM conversations"
        " WHERE ended_utc >= ? AND chaos_rating IS NOT NULL ORDER BY chaos_rating DESC",
        (since,)).fetchall()
    if not rows:
        lines.append("  (none finished in the last 24h)")
    for server, channel, sq, eq, mc, pc, rating, ev in rows:
        lines.append(f"  [{rating}/1000] {server} {channel} ({sq} -> {eq}, "
                     f"{mc} msgs, {pc} participants)")
        lines.append(f"    {ev}")
    lines.append("")
    top = conn.execute(
        "SELECT MAX(chaos_rating) FROM conversations WHERE chaos_rating IS NOT NULL"
    ).fetchone()[0]
    lines.append(f"All-time top chaos rating: {top if top is not None else 'n/a'}")
    return "\n".join(lines)


def drive_backup():
    """Upload newton.db to Drive, reusing the same file. Returns (file_id, url)."""
    state = {}
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH) as f:
            state = json.load(f)
    file_id = state.get("file_id")
    if file_id:
        res = subprocess.run(
            ["hatch_gws_cli", "drive", "files", "update",
             "--params", json.dumps({"fileId": file_id}),
             "--upload", "./newton.db"],
            cwd=BASE, capture_output=True, text=True, timeout=180)
        if res.returncode != 0:
            raise RuntimeError(f"Drive update failed: {res.stderr.strip()[:500]}")
        out = json.loads(res.stdout) if res.stdout.strip() else {}
        fid = out.get("id", file_id)
    else:
        res = subprocess.run(
            ["hatch_gws_cli", "drive", "+upload", "./newton.db",
             "--parent", MEM_FOLDER_ID, "--name", BACKUP_NAME],
            cwd=BASE, capture_output=True, text=True, timeout=180)
        if res.returncode != 0:
            raise RuntimeError(f"Drive upload failed: {res.stderr.strip()[:500]}")
        out = json.loads(res.stdout) if res.stdout.strip() else {}
        fid = out.get("id")
        if not fid:
            raise RuntimeError(f"Drive upload returned no id: {res.stdout[:500]}")
        with open(STATE_PATH, "w") as f:
            json.dump({"file_id": fid, "name": BACKUP_NAME}, f, indent=1)
    return fid, f"https://drive.google.com/file/d/{fid}/view?usp=drivesdk"


def progress_trend():
    """Regenerate PROGRESS.md and return its trend section for the report.

    Best-effort: a failure here must never break the daily report.
    """
    try:
        subprocess.run([sys.executable, os.path.join(BASE, "progress.py")],
                       cwd=BASE, capture_output=True, timeout=120)
        res = subprocess.run(
            [sys.executable, os.path.join(BASE, "progress.py"), "--trend"],
            cwd=BASE, capture_output=True, text=True, timeout=120)
        if res.returncode == 0 and res.stdout.strip():
            return res.stdout.strip()
    except Exception as e:
        return f"(trend summary unavailable: {e})"
    return "(trend summary unavailable)"


def main():
    conn = newton_db.connect(DB_PATH)
    n_closed = finalize_stale(conn)
    report = build_report(conn)
    report += "\n\nPROGRESS TREND (from scored interactions)\n" + progress_trend()
    report_path = os.path.join(BASE, "daily_report_latest.txt")
    with open(report_path, "w") as f:
        f.write(report + "\n")
    print(report)
    print(f"\n[stale conversations finalized: {n_closed}]")
    try:
        fid, url = drive_backup()
        print(f"[Drive backup OK] file_id={fid}")
        print(f"[Drive backup link] {url}")
    except Exception as e:
        print(f"[Drive backup FAILED] {e}")


if __name__ == "__main__":
    main()
