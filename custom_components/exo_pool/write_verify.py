"""Check that the eXO actually applied a pool write, and resend if not.

AWS IoT accepting a desired-state update does not mean the eXO acted on it.
In the field the device now and then ignores a delta (seen with production
on/off, which drives the filter pump), while the shadow keeps reporting the
old value. Without a check, HA shows the old state and the command is lost.

This module has no Home Assistant imports so it can be tested on its own.
"""
from __future__ import annotations

import asyncio
import copy
import logging
import time
from typing import Awaitable, Callable

_LOGGER = logging.getLogger(__name__)

VERIFY_TIMEOUT = 20.0  # device normally reports back within 2-8 s
VERIFY_ATTEMPTS = 3  # first send + 2 resends
STATE_REQUEST_WAIT = 5.0  # extra wait after asking for a fresh shadow
PENDING_MAX_AGE = 120.0  # never hide the real state longer than this


class PendingWrites:
    """Keep showing a sent value until the device reports it.

    Right after a write, AWS IoT echoes the shadow back with the old
    reported value, so a switch went off -> on -> off in the UI. While a
    write is pending, incoming reports get the sent value laid over them.
    """

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._pending: dict[str, tuple[list[str], object, float]] = {}

    def add(self, key: str, keys: list[str], value) -> None:
        self._pending[key] = (keys, value, self._clock())

    def clear(self, key: str, value=None) -> None:
        """Drop a pending value. With value, only if it is still that value."""
        entry = self._pending.get(key)
        if entry and (value is None or entry[1] == value):
            del self._pending[key]

    def apply(self, reported: dict) -> dict:
        """Return reported with any still-pending values laid over it."""
        if not self._pending:
            return reported
        now = self._clock()
        result = reported
        for key, (keys, value, since) in list(self._pending.items()):
            if now - since > PENDING_MAX_AGE or reported_matches(reported, keys, value):
                del self._pending[key]
                continue
            if result is reported:
                result = copy.deepcopy(reported)
            node = result
            for part in keys[:-1]:
                node = node.setdefault(part, {})
            node[keys[-1]] = value
        return result


def _lookup(data: dict | None, keys: list[str]):
    node = data
    for key in keys:
        if not isinstance(node, dict) or key not in node:
            return None
        node = node[key]
    return node


def reported_matches(reported: dict | None, keys: list[str], desired) -> bool:
    """Return True when the reported shadow shows the desired value.

    On/off fields are tri-state on some keys: production reports 2 when
    the controller has paused the cell on a fault, which still means "on".
    """
    value = _lookup(reported, keys)
    if value is None:
        return False
    if desired in (0, 1) and isinstance(value, int) and not isinstance(value, bool):
        return (value != 0) == bool(desired)
    return value == desired


class ReportedWatcher:
    """Fan out reported-state updates from MQTT to anyone waiting on them."""

    def __init__(self) -> None:
        self._waiters: set[Callable[[dict], None]] = set()

    def notify(self, reported: dict) -> None:
        for waiter in list(self._waiters):
            waiter(reported)

    async def wait_for(
        self, keys: list[str], desired, timeout: float
    ) -> bool:
        """Wait until a reported update matches, or the timeout passes."""
        loop = asyncio.get_running_loop()
        matched = loop.create_future()

        def _check(reported: dict) -> None:
            if not matched.done() and reported_matches(reported, keys, desired):
                matched.set_result(True)

        self._waiters.add(_check)
        try:
            await asyncio.wait_for(matched, timeout)
            return True
        except asyncio.TimeoutError:
            return False
        finally:
            self._waiters.discard(_check)


async def verify_write(
    watcher: ReportedWatcher,
    keys: list[str],
    desired,
    *,
    resend: Callable[[], Awaitable[None]],
    request_state: Callable[[], None],
    label: str,
    timeout: float = VERIFY_TIMEOUT,
    attempts: int = VERIFY_ATTEMPTS,
    state_request_wait: float = STATE_REQUEST_WAIT,
) -> bool:
    """Wait for the device to report the desired value; resend if it doesn't.

    The first send has already happened when this is called. Only reports
    that arrive after the call count, so the optimistic value HA writes into
    its own data can't fool the check. Returns False if every attempt failed.
    """
    for attempt in range(1, attempts + 1):
        if await watcher.wait_for(keys, desired, timeout):
            if attempt > 1:
                _LOGGER.warning("%s applied after resend %s", label, attempt - 1)
            return True
        # Maybe the report got lost rather than the command: ask for the shadow.
        request_state()
        if await watcher.wait_for(keys, desired, state_request_wait):
            return True
        if attempt < attempts:
            _LOGGER.warning(
                "%s not applied by the device after %.0fs, resending (%s/%s)",
                label,
                timeout + state_request_wait,
                attempt,
                attempts - 1,
            )
            await resend()
    _LOGGER.error(
        "%s still not applied by the device after %s sends", label, attempts
    )
    return False
