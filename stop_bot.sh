#!/bin/bash
# Gracefully stop Newton: SIGTERM -> the bot sends QUIT (freeing the nick
# immediately) instead of dying silently and leaving a ghost nick behind
# (a ghost nick is what caused the Newton_ split-nick race).
# Never use kill -9 on the bot: it cannot clean up after that.
cd /home/hatch/workspace/newton-irc
if [ -f newton_bot.pid ] && kill -0 "$(cat newton_bot.pid)" 2>/dev/null; then
  kill -TERM "$(cat newton_bot.pid)"
  echo "SIGTERM sent to pid $(cat newton_bot.pid); bot will QUIT cleanly"
else
  echo "bot not running"
fi
