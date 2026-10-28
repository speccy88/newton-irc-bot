#!/usr/bin/env python3
"""Offline tests for the 2026-10-28 participation gate.

Run:  ~/workspace/newton-irc/venv/bin/python -m unittest tests.test_gate -v
(or: venv/bin/python tests/test_gate.py)

Everything runs against temp DB copies and temp banned/latched files —
never the live newton.db, banned.json, latched.json, or a connection.
The gate functions are pure; _maybe_queue needs only a NetworkBot
shell (no sockets: NetworkBot.__init__ + direct method calls only).
"""
import os
import sys
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import newton_bot
import newton_db


def _bot(tmpdb, tmpdir, banned=None):
    """NetworkBot shell on a temp DB, with temp gate files.

    Module-global BANNED_FILE/LATCHED_FILE are rebound per test so no
    test ever touches the live files.
    """
    newton_bot.LATCHED_FILE = os.path.join(tmpdir, "latched.json")
    newton_bot.BANNED_FILE = os.path.join(tmpdir, "banned.json")
    bot = newton_bot.NetworkBot("rizon", {}, tmpdb, {}, banned or {})
    bot.channels["#robots"] = newton_bot.ChannelState("#robots")
    return bot


def _msg(tmpdb, sender, text, direction="inbound", ts=None):
    """Insert a message row directly; returns its id."""
    ts_utc = ts or datetime.now(timezone.utc).isoformat()
    cur = tmpdb.execute(
        "INSERT INTO messages (server, channel, sender, direction, text,"
        " ts_utc, ts_quebec) VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("rizon", "#robots", sender, direction, text, ts_utc, ts_utc),
    )
    tmpdb.commit()
    return cur.lastrowid


class GateAddressingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = newton_db.connect(os.path.join(self.tmp.name, "t.db"))
        self.bot = _bot(self.db, self.tmp.name)
        self.st = self.bot.channels["#robots"]
        # Channel regulars, so leading "julia," counts as an addressee.
        self.st.active_members.update({"julia", "de-facto", "clnkvx", "Ni8mare"})

    def tearDown(self):
        self.tmp.cleanup()

    def _queue(self, sender, text):
        mid = _msg(self.db, sender, text)
        self.bot._maybe_queue("#robots", sender, text, mid)
        return mid

    def _pending(self):
        return newton_db.get_pending_interactions(self.db)

    def _decisions(self):
        return self.db.execute(
            "SELECT decision, reason FROM gate_decisions ORDER BY id").fetchall()

    def test_direct_leading_address_queues(self):
        self._queue("de-facto", "Newton, which model are you running?")
        rows = self._pending()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["gate"], "addressed")
        self.assertEqual(self._decisions(), [("queued", "addressed")])

    def test_direct_trailing_vocative_queues(self):
        self._queue("clnkvx", "what do you think, Newton?")
        self.assertEqual(len(self._pending()), 1)
        self.assertEqual(self._pending()[0]["gate"], "addressed")

    def test_direct_address_to_current_nick(self):
        self.bot.nick = "Newton_"
        self._queue("de-facto", "Newton_, what do you think?")
        self.assertEqual(len(self._pending()), 1)

    def test_julia_addressed_comma_skipped(self):
        self._queue("de-facto",
                    "julia, if we assume that training maps noisy semantic space")
        self.assertEqual(self._pending(), [])
        self.assertEqual(self._decisions(), [("skipped", "other-addressee")])

    def test_julia_addressed_colon_skipped(self):
        self._queue("de-facto",
                    "julia: what if we think of a sequence of tokens")
        self.assertEqual(self._pending(), [])
        self.assertEqual(self._decisions(), [("skipped", "other-addressee")])

    def test_topical_only_skipped(self):
        self._queue("julia",
                    "essentially you're describing the latent space as a phase space")
        self.assertEqual(self._pending(), [])
        self.assertEqual(self._decisions(), [("skipped", "topical-only")])

    def test_action_not_an_address(self):
        self._queue("clnkvx",
                    "\x01ACTION drop kicks Newton all the way to Redmond, WA\x01")
        self.assertEqual(self._pending(), [])
        self.assertEqual(self._decisions(), [("skipped", "topical-only")])

    def test_leading_common_word_not_an_addressee(self):
        self._queue("de-facto", "well, the thing is that latent space is jagged")
        self.assertEqual(self._decisions(), [("skipped", "topical-only")])

    def test_continuation_by_prior_speaker(self):
        self.st.last_newton_msg = time.time()
        self.st.last_newton_text = "interesting, tell me more"
        self.st.speakers_since_newton = {"de-facto"}
        self._queue("de-facto", "yeah and the attractor idea follows from that")
        rows = self._pending()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["gate"], "continuation")

    def test_continuation_by_answering_newton_question(self):
        self.st.last_newton_msg = time.time()
        self.st.last_newton_text = "what do you think happens next?"
        self._queue("clnkvx", "probably it collapses back to 1=1")
        self.assertEqual(len(self._pending()), 1)
        self.assertEqual(self._pending()[0]["gate"], "continuation")

    def test_continuation_by_immediate_reply(self):
        # The clnkvx 05:27 correction shape: Newton's line is the last
        # message in the channel, 2 min old, and clnkvx replies to it
        # directly — that IS continuing the exchange, even though
        # Newton's line was not a question and clnkvx had not spoken
        # since Newton spoke.
        self.st.last_newton_msg = time.time() - 120
        self.st.last_newton_text = ("alternate spaces? brilliant, right up "
                                    "until some text editor auto-corrects it")
        self.st.last_msg_from_newton = True
        self._queue("clnkvx",
                    "it's just a proposal for a convention, this is not "
                    "steganography")
        self.assertEqual(len(self._pending()), 1)
        self.assertEqual(self._pending()[0]["gate"], "continuation")

    def test_immediate_reply_expires(self):
        self.st.last_newton_msg = time.time() - 301
        self.st.last_newton_text = "interesting, tell me more"
        self.st.last_msg_from_newton = True
        self._queue("clnkvx", "the attractor idea is interesting")
        self.assertEqual(self._pending(), [])

    def test_no_continuation_after_window(self):
        self.st.last_newton_msg = time.time() - newton_bot.THREAD_WINDOW_S - 60
        self.st.speakers_since_newton = {"de-facto"}
        self._queue("de-facto", "yeah and the attractor idea follows from that")
        self.assertEqual(self._pending(), [])

    def test_current_message_does_not_self_continue(self):
        # Inbound order matters: _on_inbound records the speaker AFTER
        # the gate decision, so a first message in a live thread from
        # a fresh speaker is NOT a continuation by itself.
        self.st.last_newton_msg = time.time()
        self.st.last_newton_text = "interesting, tell me more"
        self._queue("Ni8mare", "the singularity paper says otherwise")
        self.assertEqual(self._pending(), [])


class LatchTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = newton_db.connect(os.path.join(self.tmp.name, "t.db"))
        self.bot = _bot(self.db, self.tmp.name)
        self.st = self.bot.channels["#robots"]
        self.st.active_members.update({"fenn", "de-facto"})

    def tearDown(self):
        self.tmp.cleanup()

    def test_fenn_callout_latches_and_closes_pending(self):
        # A pending trigger exists when the complaint arrives.
        mid = _msg(self.db, "de-facto", "Newton, which model are you running?")
        self.bot._maybe_queue("#robots", "de-facto",
                              "Newton, which model are you running?", mid)
        self.assertEqual(len(newton_db.get_pending_interactions(self.db)), 1)
        cmid = _msg(self.db, "fenn",
                    "whoever's running newton, please don't have your bot "
                    "autorespond like that")
        self.bot._maybe_queue("#robots", "fenn",
                              "whoever's running newton, please don't have "
                              "your bot autorespond like that", cmid)
        self.assertEqual(newton_db.get_pending_interactions(self.db), [])
        latched = newton_bot.load_latched()
        self.assertIn("#robots", latched.get("rizon", {}))
        self.assertEqual(
            self.db.execute("SELECT decision FROM gate_decisions ORDER BY id DESC"
                            " LIMIT 1").fetchone()[0], "latched")

    def test_latched_channel_still_answers_direct_address(self):
        newton_bot.set_latch("rizon", "#robots", "test latch", 1)
        mid = _msg(self.db, "de-facto", "Newton, are you still there?")
        self.bot._maybe_queue("#robots", "de-facto",
                              "Newton, are you still there?", mid)
        rows = newton_db.get_pending_interactions(self.db)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["gate"], "latched-addressed")

    def test_latched_channel_blocks_topical(self):
        newton_bot.set_latch("rizon", "#robots", "test latch", 1)
        mid = _msg(self.db, "de-facto", "the attractor idea is interesting")
        self.bot._maybe_queue("#robots", "de-facto",
                              "the attractor idea is interesting", mid)
        self.assertEqual(newton_db.get_pending_interactions(self.db), [])
        self.assertEqual(
            self.db.execute("SELECT reason FROM gate_decisions ORDER BY id DESC"
                            " LIMIT 1").fetchone()[0], "latched")

    def test_bare_shut_up_about_someone_else_does_not_latch(self):
        mid = _msg(self.db, "de-facto", "shut up julia, nobody asked")
        self.bot._maybe_queue("#robots", "de-facto",
                              "shut up julia, nobody asked", mid)
        self.assertNotIn("#robots", newton_bot.load_latched().get("rizon", {}))


class WarningPatternTests(unittest.TestCase):
    def test_stop_being_human_false_positive_fixed(self):
        text = "just to stop being human it seems to be a signal"
        self.assertFalse(any(p.search(text)
                             for p in newton_bot.WARNING_PATTERNS))

    def test_real_stop_requests_still_match(self):
        for text in ("stop it.", "quit it!", "stop spamming please",
                     "stop flooding the channel"):
            self.assertTrue(any(p.search(text)
                                for p in newton_bot.WARNING_PATTERNS), text)


class BanTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = newton_db.connect(os.path.join(self.tmp.name, "t.db"))
        self.bot = _bot(self.db, self.tmp.name)
        self.bot.channels["#vtluug"] = newton_bot.ChannelState("#vtluug")

    def tearDown(self):
        self.tmp.cleanup()

    def test_join_fail_474_persists_ban(self):
        self.bot._on_join_fail("474", "#vtluug", "Cannot join channel (+b)")
        self.assertIn("#vtluug", self.bot.banned)
        self.assertIn("#vtluug", newton_bot.load_banned().get("rizon", []))
        ev = self.db.execute(
            "SELECT kind FROM events WHERE channel = '#vtluug'"
            " ORDER BY id DESC LIMIT 1").fetchone()
        self.assertEqual(ev[0], "JOIN_FAIL_474")

    def test_other_join_failures_do_not_persist(self):
        self.bot._on_join_fail("473", "#vtluug", "Cannot join channel (+i)")
        self.assertNotIn("#vtluug", self.bot.banned)
        self.assertNotIn("#vtluug", newton_bot.load_banned().get("rizon", []))

    def test_banned_channel_never_queues(self):
        self.bot.banned.add("#vtluug")
        mid = _msg(self.db, "someone", "Newton, hello?")
        # _msg writes to #robots; queue for #vtluug must refuse anyway.
        self.bot._maybe_queue("#vtluug", "someone", "Newton, hello?", mid)
        self.assertEqual(newton_db.get_pending_interactions(self.db), [])


class StalenessTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = newton_db.connect(os.path.join(self.tmp.name, "t.db"))

    def tearDown(self):
        self.tmp.cleanup()

    def test_old_trigger_is_stale(self):
        old = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        mid = _msg(self.db, "de-facto", "Newton, hello?", ts=old)
        ts = newton_db.get_message_ts(self.db, mid)
        age = (datetime.now(timezone.utc)
               - datetime.fromisoformat(ts)).total_seconds()
        self.assertGreater(age, newton_bot.GATE_STALE_TRIGGER_S)

    def test_fresh_trigger_is_not_stale(self):
        mid = _msg(self.db, "de-facto", "Newton, hello?")
        ts = newton_db.get_message_ts(self.db, mid)
        age = (datetime.now(timezone.utc)
               - datetime.fromisoformat(ts)).total_seconds()
        self.assertLess(age, newton_bot.GATE_STALE_TRIGGER_S)


class ConversationOverlapTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = newton_db.connect(os.path.join(self.tmp.name, "t.db"))

    def tearDown(self):
        self.tmp.cleanup()

    def test_reopen_closes_prior_window_no_double_count(self):
        a = newton_db.open_conversation(self.db, "rizon", "#robots")
        for i in range(3):
            _msg(self.db, "de-facto", f"message {i}")
        # Simulate a restart: a fresh open must close A first.
        b = newton_db.open_conversation(self.db, "rizon", "#robots")
        for i in range(2):
            _msg(self.db, "julia", f"later {i}")
        newton_db.finish_conversation(self.db, b)
        rows = self.db.execute(
            "SELECT message_count FROM conversations ORDER BY id").fetchall()
        counts = [r[0] for r in rows]
        # No message counted twice: A=3, B=2, total 5.
        self.assertEqual(counts, [3, 2])
        self.assertEqual(sum(counts), 5)
        ended_a = self.db.execute(
            "SELECT ended_utc FROM conversations WHERE id = ?", (a,)).fetchone()[0]
        started_b = self.db.execute(
            "SELECT started_utc FROM conversations WHERE id = ?", (b,)).fetchone()[0]
        self.assertLessEqual(ended_a, started_b)


class MetricsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = newton_db.connect(os.path.join(self.tmp.name, "t.db"))
        self.since = (datetime.now(timezone.utc)
                      - timedelta(hours=24)).isoformat()

    def tearDown(self):
        self.tmp.cleanup()

    def test_gate_metrics(self):
        # One addressed answer, one continuation answer, one addressed
        # question left skipped, one latch, one peer follow-up. Each
        # interaction gets its own channel: queueing merges bursts per
        # channel, so one channel would collapse all three rows.
        now = datetime.now(timezone.utc)
        for channel, gate, status in (
                ("#robots", "addressed", "answered"),
                ("#corsair", "continuation", "answered"),
                ("#encrypt", "addressed", "skipped")):
            iid = newton_db.queue_or_merge_interaction(
                self.db, "rizon", channel, 1, "de-facto", "hi", gate=gate)
            newton_db.mark_interaction(
                self.db, iid, status,
                reply_text="r" if status == "answered" else None,
                skip_reason=None if status == "answered" else "x")
        newton_db.log_gate_decision(self.db, "rizon", "#robots", 9,
                                    "fenn", "stop autoresponding",
                                    "latched", "complaint about Newton")
        _msg(self.db, "Newton", "hello there", direction="outbound",
             ts=now.isoformat())
        _msg(self.db, "de-facto", "oh hey, interesting",
             ts=(now + timedelta(seconds=60)).isoformat())
        m = newton_db.gate_metrics(self.db, self.since)
        self.assertEqual(m["answered_total"], 2)
        self.assertEqual(m["addressed"], 1)
        self.assertEqual(m["addressed_pct"], 50.0)
        self.assertEqual(m["peer_followups"], 1)
        self.assertEqual(m["latched"], 1)
        self.assertEqual(m["unanswered_direct"], 1)
        self.assertEqual(m["gate_breakdown"], {"latched": 1})


if __name__ == "__main__":
    unittest.main()
