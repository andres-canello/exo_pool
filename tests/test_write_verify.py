"""Tests for write_verify: checking that the eXO applied a write.

Stdlib only (unittest), so they also run without pytest or HA installed.
"""
from __future__ import annotations

import asyncio
import importlib.util
import pathlib
import sys
import unittest

_PATH = (
    pathlib.Path(__file__).parent.parent
    / "custom_components"
    / "exo_pool"
    / "write_verify.py"
)
_spec = importlib.util.spec_from_file_location("exo_write_verify", _PATH)
wv = importlib.util.module_from_spec(_spec)
sys.modules["exo_write_verify"] = wv
_spec.loader.exec_module(wv)

KEYS = ["equipment", "swc_0", "production"]


def _reported(production) -> dict:
    return {"equipment": {"swc_0": {"production": production}}}


class TestReportedMatches(unittest.TestCase):
    def test_on_off(self):
        self.assertTrue(wv.reported_matches(_reported(0), KEYS, 0))
        self.assertTrue(wv.reported_matches(_reported(1), KEYS, 1))
        self.assertFalse(wv.reported_matches(_reported(1), KEYS, 0))
        self.assertFalse(wv.reported_matches(_reported(0), KEYS, 1))

    def test_production_paused_by_fault_counts_as_on(self):
        self.assertTrue(wv.reported_matches(_reported(2), KEYS, 1))
        self.assertFalse(wv.reported_matches(_reported(2), KEYS, 0))

    def test_numbers_must_be_equal(self):
        keys = ["equipment", "swc_0", "swc"]
        data = {"equipment": {"swc_0": {"swc": 50}}}
        self.assertTrue(wv.reported_matches(data, keys, 50))
        self.assertFalse(wv.reported_matches(data, keys, 40))

    def test_nested_aux(self):
        keys = ["equipment", "swc_0", "aux_1", "state"]
        data = {"equipment": {"swc_0": {"aux_1": {"state": 1}}}}
        self.assertTrue(wv.reported_matches(data, keys, 1))

    def test_missing_key(self):
        self.assertFalse(wv.reported_matches({}, KEYS, 0))
        self.assertFalse(wv.reported_matches(None, KEYS, 0))


class TestPendingWrites(unittest.TestCase):
    def setUp(self):
        self.now = 0.0
        self.pending = wv.PendingWrites(clock=lambda: self.now)

    def _production(self, data):
        return data["equipment"]["swc_0"]["production"]

    def test_echo_of_old_state_is_hidden(self):
        """The 2026-10-31 16:37 flicker: off, echo says on, then off."""
        self.pending.add("pool:production", KEYS, 0)
        echo = _reported(1)
        shown = self.pending.apply(echo)
        self.assertEqual(self._production(shown), 0)
        self.assertEqual(self._production(echo), 1)  # input not changed

    def test_cleared_once_device_reports_it(self):
        self.pending.add("pool:production", KEYS, 0)
        self.assertEqual(self._production(self.pending.apply(_reported(0))), 0)
        # Later the device really turns on again (e.g. its own schedule).
        self.assertEqual(self._production(self.pending.apply(_reported(1))), 1)

    def test_expires(self):
        self.pending.add("pool:production", KEYS, 0)
        self.now = wv.PENDING_MAX_AGE + 1
        self.assertEqual(self._production(self.pending.apply(_reported(1))), 1)

    def test_clear_only_matching_value(self):
        self.pending.add("pool:production", KEYS, 1)  # newer write
        self.pending.clear("pool:production", 0)  # older check finishing
        self.assertEqual(self._production(self.pending.apply(_reported(0))), 1)
        self.pending.clear("pool:production", 1)
        self.assertEqual(self._production(self.pending.apply(_reported(0))), 0)

    def test_nothing_pending_returns_same_object(self):
        data = _reported(1)
        self.assertIs(self.pending.apply(data), data)


class TestVerifyWrite(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.watcher = wv.ReportedWatcher()
        self.resends = 0
        self.state_requests = 0

    async def _resend(self):
        self.resends += 1

    def _request_state(self):
        self.state_requests += 1

    def _verify(self, **kwargs):
        return wv.verify_write(
            self.watcher,
            KEYS,
            0,
            resend=self._resend,
            request_state=self._request_state,
            label="Write production=0",
            timeout=kwargs.pop("timeout", 0.05),
            state_request_wait=kwargs.pop("state_request_wait", 0.02),
            **kwargs,
        )

    async def test_device_applies_first_time(self):
        task = asyncio.create_task(self._verify())
        await asyncio.sleep(0)
        self.watcher.notify(_reported(1))  # cloud echo of the old state
        self.watcher.notify(_reported(0))  # device applied it
        self.assertTrue(await task)
        self.assertEqual(self.resends, 0)
        self.assertEqual(self.state_requests, 0)

    async def test_ignored_command_is_resent(self):
        """The 2026-10-31 08:54 case: echo only, device never acts."""
        task = asyncio.create_task(self._verify())
        await asyncio.sleep(0)
        self.watcher.notify(_reported(1))
        # First wait + state request run out, then a resend goes out.
        while self.resends == 0:
            await asyncio.sleep(0.01)
        self.watcher.notify(_reported(0))
        self.assertTrue(await task)
        self.assertEqual(self.resends, 1)
        self.assertEqual(self.state_requests, 1)

    async def test_lost_report_found_by_state_request(self):
        def _request_state():
            self.state_requests += 1
            asyncio.get_running_loop().call_soon(
                self.watcher.notify, _reported(0)
            )

        result = await wv.verify_write(
            self.watcher,
            KEYS,
            0,
            resend=self._resend,
            request_state=_request_state,
            label="Write production=0",
            timeout=0.05,
            state_request_wait=0.5,
        )
        self.assertTrue(result)
        self.assertEqual(self.resends, 0)

    async def test_gives_up_after_all_attempts(self):
        self.assertFalse(await self._verify(attempts=3))
        self.assertEqual(self.resends, 2)
        self.assertEqual(self.state_requests, 3)

    async def test_reports_before_the_check_starts_do_not_count(self):
        self.watcher.notify(_reported(0))
        self.assertFalse(await self._verify(attempts=1))

    async def test_waiters_are_removed(self):
        await self._verify(attempts=1)
        self.assertEqual(len(self.watcher._waiters), 0)

    async def test_cancel_stops_resending(self):
        task = asyncio.create_task(self._verify(timeout=1.0))
        await asyncio.sleep(0.01)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.resends, 0)
        self.assertEqual(len(self.watcher._waiters), 0)


if __name__ == "__main__":
    unittest.main()
