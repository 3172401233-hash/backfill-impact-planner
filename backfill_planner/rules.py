"""Partition predicates: how upstream partitions map to downstream partitions."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Callable, Iterable, Optional


class PartitionRule:
    """Maps partitions between an upstream and a downstream dataset.

    ``forward`` answers: given a changed upstream partition, which downstream
    partitions are impacted. Returning nothing blocks the downstream chain.

    ``required`` answers: to rebuild one downstream partition, which upstream
    partitions must exist (materialized, planned, or freshly changed). It is
    used for materialization-gap / missing-version checks and defaults to
    "only the partitions that triggered this task".
    """

    def forward(self, upstream_partition: str) -> Iterable[str]:
        raise NotImplementedError

    def required(self, downstream_partition: str) -> Iterable[str]:
        return ()


class FunctionRule(PartitionRule):
    def __init__(
        self,
        forward: Callable[[str], Iterable[str]],
        required: Optional[Callable[[str], Iterable[str]]] = None,
    ):
        self._forward = forward
        self._required = required

    def forward(self, upstream_partition: str) -> Iterable[str]:
        return self._forward(upstream_partition)

    def required(self, downstream_partition: str) -> Iterable[str]:
        if self._required is None:
            return ()
        return self._required(downstream_partition)


def identity_rule() -> PartitionRule:
    """Same partition value on both sides (e.g. day -> day)."""
    return FunctionRule(lambda p: (p,))


def hourly_to_daily_rule() -> PartitionRule:
    """Hour partition 'YYYY-MM-DDTHH' rolls up into day 'YYYY-MM-DD'.

    Rebuilding one day requires all 24 hour partitions of that day.
    """

    def forward(hour: str) -> Iterable[str]:
        if "T" not in hour:
            return ()
        return (hour.split("T")[0],)

    def required(day: str) -> Iterable[str]:
        return tuple(f"{day}T{h:02d}" for h in range(24))

    return FunctionRule(forward, required)


def daily_rolling_rule(window: int = 2) -> PartitionRule:
    """Cross-day aggregation: day D covers the ``window`` days ending at D.

    A change on day D impacts D .. D+window-1; rebuilding day D requires
    D-window+1 .. D upstream partitions.
    """
    if window < 1:
        raise ValueError("window must be >= 1")

    def forward(day: str) -> Iterable[str]:
        base = date.fromisoformat(day)
        return tuple((base + timedelta(days=i)).isoformat() for i in range(window))

    def required(day: str) -> Iterable[str]:
        base = date.fromisoformat(day)
        return tuple((base - timedelta(days=i)).isoformat() for i in range(window))

    return FunctionRule(forward, required)
