#!/bin/bash
# Start Newton IRC bot if not already running (PID-file guarded).
cd /home/hatch/workspace/newton-irc
if [ -f newton_bot.pid ] && kill -0 "$(cat newton_bot.pid)" 2>/dev/null; then
  echo "already running pid $(cat newton_bot.pid)"
  exit 0
fi
nohup python3 newton_bot.py > newton_bot.log 2>&1 &
echo "started pid $!"
