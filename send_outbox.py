#!/usr/bin/env python3
"""Queue a directed message for Newton to send as himself (no --once).

Usage: python3 send_outbox.py <server> <channel> <message>

Appends {"server","channel","text"} to outbox.jsonl. The persistent bot
picks it up within ~10s and sends it through its own connection
(rate-limited, logged as outbound) — so the message goes out as Newton
with no nick collision.

Only use while the persistent bot is running. NEVER run --once
concurrently with the persistent bot: the one-shot connection collides
on the nick (Newton_) and its posts get echoed back as fake inbound.
For stopping the bot, use stop_bot.sh (SIGTERM -> clean QUIT).
"""
import json
import os
import sys

BASE = os.path.dirname(os.path.abspath(__file__))
OUTBOX = os.path.join(BASE, "outbox.jsonl")

if len(sys.argv) < 4:
    print("usage: send_outbox.py <server> <channel> <message>")
    sys.exit(2)

entry = {"server": sys.argv[1], "channel": sys.argv[2],
         "text": " ".join(sys.argv[3:])}
with open(OUTBOX, "a") as f:
    f.write(json.dumps(entry) + "\n")
print(f"queued for {entry['server']} {entry['channel']}: {entry['text'][:80]}")
