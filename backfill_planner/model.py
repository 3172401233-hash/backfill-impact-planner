"""Data model for the backfill impact planner."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, FrozenSet, Optional, Tuple

from .rules import PartitionRule


@dataclass(frozen=True)
class Dataset:
    name: str
    partition_keys: Tuple[str, ...] = ()
    known_partitions: Optional[FrozenSet[str]] = None
    refresh: bool = True


@dataclass(frozen=True)
class Dependency:
    upstream: str
    downstream: str
    column_map: Dict[str, FrozenSet[str]]
    rule: PartitionRule


@dataclass(frozen=True)
class ChangeEvent:
    dataset: str
    partitions: Tuple[str, ...]
    columns: Optional[Tuple[str, ...]] = None


@dataclass(frozen=True)
class MaterializedPartition:
    dataset: str
    partition: str
    version: str = "1"


@dataclass(frozen=True)
class Task:
    dataset: str
    partition: str
    columns: Optional[Tuple[str, ...]] = None


@dataclass(frozen=True)
class BlockedTask:
    dataset: str
    partition: Optional[str]
    reason: str


@dataclass(frozen=True)
class Plan:
    tasks: Tuple[Task, ...] = ()
    blocked: Tuple[BlockedTask, ...] = ()
