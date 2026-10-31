"""Check that the eXO actually applied a pool write, and resend if not.

AWS IoT accepting a desired-state update does not mean the eXO acted on it.
In the field the device now and then ignores a delta (seen with production
on/off, which drives the filter pump), while the shadow keeps reporting the
old value. Without a check, HA shows the old state and the command is lost.

This module has no Home Assistant imports so it can be tested on its own.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable

_LOGGER = logging.getLogger(__name__)

VERIFY_TIMEOUT = 20.0  # device normally reports back within 2-8 s
VERIFY_ATTEMPTS = 3  # first send + 2 resends
STATE_REQUEST_WAIT = 5.0  # extra wait after asking for a fresh shadow


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
