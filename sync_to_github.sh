#!/bin/bash
# sync_to_github.sh — Newton IRC bot -> GitHub sync (safe by construction).
#
# What it does, in order:
#   1. Stages tracked changes (git add -u) plus explicitly-listed source
#      files (so a stray file dropped in the directory is never staged).
#      .gitignore already excludes runtime state (see below).
#   2. DENYLIST AUDIT on the staged set — aborts (and unstages) if any
#      staged path matches a forbidden pattern OR any staged content
#      matches a secret pattern. No push happens on audit failure.
#   3. Commits only if something is staged, with a message describing
#      the change (file list + stat summary).
#   4. Pushes with a plain `git push` (never --force, never --mirror).
#      In environments without git-remote credentials (e.g. this Muse
#      VM, where gh is not logged in), the push step fails cleanly and
#      the local commit still stands; the hourly cron worker then uses
#      the GitHub connector (push_files) as the transport for the same
#      audited commit. The script never starts its own auth flow.
#
# Exit codes: 0 = pushed, or nothing to commit
#             2 = denylist audit abort (nothing pushed, nothing committed)
#             3 = committed locally but git push failed (see log)
# Log: sync.log in this directory (gitignored).
set -u
cd "$(dirname "$0")"
LOG="sync.log"
{
echo "=== sync run $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
} >> "$LOG"

# --- 1. Stage only tracked changes + explicitly intended files --------
git add -u >> "$LOG" 2>&1
for f in .gitignore LICENSE README.md requirements.txt config.example.json \
         newton_bot.py newton_brain.py newton_db.py progress.py \
         daily_report.py send_outbox.py start_bot.sh stop_bot.sh \
         sync_to_github.sh soul/SOUL.md soul/IDENTITY.md soul/README.md; do
    [ -f "$f" ] && git add -- "$f" >> "$LOG" 2>&1
done

STAGED=$(git diff --cached --name-only)
if [ -z "$STAGED" ]; then
    echo "nothing to commit" | tee -a "$LOG"
    exit 0
fi
echo "staged:" >> "$LOG"
echo "$STAGED" >> "$LOG"

# --- 2. Denylist audit (paths AND content) — abort on any hit --------
# Forbidden paths: runtime state, live config, surveys, secrets, logs.
forbidden_path() {
    case "$1" in
        newton.db|*.sqlite|*.sqlite3|*.db|*.log|*.pid|outbox*.jsonl|\
        brain_calls.jsonl|brain_searches.jsonl|brain_prompts.jsonl|\
        drive_backup.json|day_one_watch.json|kicked.json|nick_state.json|\
        daily_report_latest.txt|PROGRESS.md|config.json|survey*.md|\
        survey*.json|survey*.py|*.bak|*.env|.env|operator_denylist.txt|\
        *.key|*.pem|venv/*|__pycache__/*|sync.log)
            return 0 ;;
        *) return 1 ;;
    esac
}
ABORT=""
while IFS= read -r f; do
    if forbidden_path "$f"; then ABORT="$ABORT forbidden-path:$f"; fi
done <<EOF
$STAGED
EOF
# Secret patterns in staged content (surrogates, API keys, private keys,
# passwords, Drive file IDs, operator identifiers are covered by the
# denylist file itself, which is never staged).
if git grep --cached -nE 'hsurr:[A-Za-z0-9]|sk-[A-Za-z0-9]{10,}' -- . >> "$LOG" 2>&1 \
   || git grep --cached -n 'BEG''IN ' -- . | grep 'PRIV''ATE KEY' >> "$LOG" 2>&1 \
   || git grep --cached -nE 'password[[:space:]]*=' -- . >> "$LOG" 2>&1; then
    ABORT="$ABORT secret-pattern-in-content"
fi
if [ -n "$ABORT" ]; then
    echo "AUDIT ABORT:$ABORT — unstaging, nothing committed, nothing pushed" | tee -a "$LOG"
    git reset -q
    exit 2
fi
echo "audit clean" >> "$LOG"

# --- 3. Commit with a describing message -------------------------------
STAT=$(git diff --cached --stat | tail -n +1)
MSG="Sync: $(echo "$STAGED" | tr '\n' ' ')"
git commit -q -m "$MSG" -m "$STAT" >> "$LOG" 2>&1
SHA=$(git rev-parse HEAD)
echo "committed $SHA" | tee -a "$LOG"

# --- 4. Push (plain, never force) ---------------------------------------
if git push origin HEAD >> "$LOG" 2>&1; then
    echo "pushed $SHA" | tee -a "$LOG"
    exit 0
else
    echo "PUSH FAILED for $SHA (no remote credentials in this environment is expected; cron worker uses connector transport)" | tee -a "$LOG"
    exit 3
fi
