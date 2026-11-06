"""Wait for a stream that uses availableNow without calling `awaitTermination()`.

On Azure serverless, `awaitTermination()` once never returned, 23 minutes after the query had finished
(the log said "Listener thread still active, was not able to reconnect"). Bronze was complete: 50,000 rows.
`wait_for` asks `isActive` every few seconds instead. See FAILURES.md ("The wait for a stream never returned on serverless").
"""

import pytest

from src.pipeline.bronze import wait_for


class FakeQuery:
    def __init__(self, active_polls, error=None):
        self.polls, self.error = active_polls, error

    @property
    def isActive(self):
        self.polls -= 1
        return self.polls > 0

    def awaitTermination(self, *args):
        raise AssertionError("awaitTermination must not be called")

    def exception(self):
        return self.error


def test_it_returns_when_the_query_is_no_longer_active_and_never_calls_await_termination():
    query = FakeQuery(active_polls=3)
    wait_for(query, poll_seconds=0)
    assert query.polls <= 0


def test_it_raises_the_error_of_a_query_that_failed():
    with pytest.raises(RuntimeError, match="boom"):
        wait_for(FakeQuery(active_polls=2, error=RuntimeError("boom")), poll_seconds=0)


def test_it_stops_waiting_after_the_limit_instead_of_waiting_for_ever():
    with pytest.raises(TimeoutError):
        wait_for(FakeQuery(active_polls=10**9), poll_seconds=0.01, limit_seconds=0.05)
