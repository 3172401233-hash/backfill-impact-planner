"""Backfill impact planner.

Given dataset dependencies, column-level lineage, partition rules, the
already-materialized partitions and the partitions that changed, compute the
minimal set of downstream (dataset, partition) tasks that must be recomputed.
"""

from __future__ import annotations

from collections import deque
from typing import Dict, Iterable, List, Optional, Set, Tuple

from .model import (
    BlockedTask,
    ChangeEvent,
    Dataset,
    Dependency,
    MaterializedPartition,
    Plan,
    Task,
)

Impact = Optional[Set[str]]  # None means "all columns" (full / partition-key change)


def plan_backfill(
    datasets: Iterable[Dataset],
    dependencies: Iterable[Dependency],
    materialized: Iterable[MaterializedPartition] = (),
    changes: Iterable[ChangeEvent] = (),
) -> Plan:
    return Planner(datasets, dependencies, materialized, changes).plan()


class Planner:
    def __init__(self, datasets, dependencies, materialized=(), changes=()):
        self._datasets = self._dedupe_datasets(datasets)
        self._dependencies = self._dedupe_dependencies(dependencies)
        self._materialized = list(materialized)
        self._changes = self._merge_changes(changes)
        self._out_edges: Dict[str, List[Dependency]] = {}
        self._in_edges: Dict[str, List[Dependency]] = {}
        for dep in self._dependencies:
            self._out_edges.setdefault(dep.upstream, []).append(dep)
            self._in_edges.setdefault(dep.downstream, []).append(dep)
        for edges in list(self._out_edges.values()) + list(self._in_edges.values()):
            edges.sort(key=lambda d: (d.upstream, d.downstream))
        self._triggers: Dict[Tuple[str, str], Set[Tuple[str, str]]] = {}

    @staticmethod
    def _dedupe_datasets(datasets: Iterable[Dataset]) -> Dict[str, Dataset]:
        by_name: Dict[str, Dataset] = {}
        for ds in datasets:
            existing = by_name.get(ds.name)
            if existing is not None and existing != ds:
                raise ValueError(f"conflicting definitions for dataset '{ds.name}'")
            by_name[ds.name] = ds
        return by_name

    def _dedupe_dependencies(self, dependencies: Iterable[Dependency]) -> List[Dependency]:
        for dep in dependencies:
            for name in (dep.upstream, dep.downstream):
                if name not in self._datasets:
                    raise ValueError(f"dependency references unknown dataset '{name}'")
        merged: Dict[Tuple[str, str], Dependency] = {}
        for dep in sorted(dependencies, key=lambda d: (d.upstream, d.downstream)):
            key = (dep.upstream, dep.downstream)
            if key not in merged:
                merged[key] = Dependency(
                    dep.upstream,
                    dep.downstream,
                    {c: frozenset(u) for c, u in dep.column_map.items()},
                    dep.rule,
                )
            else:
                target = merged[key].column_map
                for col, ups in dep.column_map.items():
                    target[col] = target.get(col, frozenset()) | frozenset(ups)
        return [merged[key] for key in sorted(merged)]

    @staticmethod
    def _merge_changes(changes: Iterable[ChangeEvent]) -> Dict[Tuple[str, str], Impact]:
        merged: Dict[Tuple[str, str], Impact] = {}
        for ev in changes:
            value: Impact = None if ev.columns is None else set(ev.columns)
            for part in ev.partitions:
                key = (ev.dataset, part)
                if key not in merged:
                    merged[key] = value
                elif merged[key] is None or value is None:
                    merged[key] = None
                else:
                    merged[key] = merged[key] | value
        return merged

    def plan(self) -> Plan:
        blocked: List[BlockedTask] = []
        impacted: Dict[str, Dict[str, Impact]] = {}
        propagated: Set[Tuple[str, str]] = set()
        seeds: Set[Tuple[str, str]] = set()
        worklist: deque = deque()

        for (ds_name, part), cols in sorted(self._changes.items()):
            ds = self._datasets.get(ds_name)
            if ds is None:
                blocked.append(BlockedTask(ds_name, part, f"unknown dataset '{ds_name}'"))
                continue
            if ds.known_partitions is not None and part not in ds.known_partitions:
                blocked.append(
                    BlockedTask(ds_name, part, f"unknown partition '{part}' for dataset '{ds_name}'")
                )
                continue
            impact = cols
            if impact is not None and set(ds.partition_keys) & impact:
                impact = None
            if self._grow(impacted, ds_name, part, impact):
                worklist.append((ds_name, part))
            seeds.add((ds_name, part))

        while worklist:
            ds_name, part = worklist.popleft()
            impact = impacted[ds_name][part]
            for dep in self._out_edges.get(ds_name, []):
                down = dep.downstream
                if impact is None:
                    down_impact: Impact = None
                else:
                    affected = {c for c, ups in dep.column_map.items() if ups & impact}
                    if not affected:
                        continue
                    down_impact = affected
                targets = sorted(set(dep.rule.forward(part) or ()))
                if not targets:
                    blocked.append(
                        BlockedTask(
                            down,
                            None,
                            f"partition rule '{ds_name}' -> '{down}' returned no "
                            f"partitions for {ds_name}@{part}",
                        )
                    )
                    continue
                dds = self._datasets[down]
                for target in targets:
                    if dds.known_partitions is not None and target not in dds.known_partitions:
                        blocked.append(
                            BlockedTask(down, target, f"unknown partition '{target}' for dataset '{down}'")
                        )
                        continue
                    self._triggers.setdefault((down, target), set()).add((ds_name, part))
                    propagated.add((down, target))
                    if self._grow(impacted, down, target, down_impact):
                        worklist.append((down, target))

        cycles = self._cyclic_members()
        tasks: Dict[Tuple[str, str], Task] = {}
        for ds_name, part in sorted(propagated):
            ds = self._datasets[ds_name]
            if not ds.refresh:
                continue
            if ds_name in cycles:
                blocked.append(
                    BlockedTask(ds_name, part, f"dependency cycle detected: {cycles[ds_name]}")
                )
                continue
            impact = impacted[ds_name][part]
            cols = None if impact is None else tuple(sorted(impact))
            tasks[(ds_name, part)] = Task(ds_name, part, cols)

        self._apply_version_checks(tasks, blocked, seeds)
        ordered = self._topo_sort(tasks)
        unique_blocked = sorted(
            set(blocked), key=lambda b: (b.dataset, b.partition or "", b.reason)
        )
        return Plan(tuple(ordered), tuple(unique_blocked))

    @staticmethod
    def _grow(impacted: Dict[str, Dict[str, Impact]], ds: str, part: str, impact: Impact) -> bool:
        slot = impacted.setdefault(ds, {})
        if part in slot:
            current = slot[part]
            if current is None:
                return False
            if impact is None:
                slot[part] = None
                return True
            grown = current | impact
            if grown == current:
                return False
            slot[part] = grown
            return True
        slot[part] = None if impact is None else set(impact)
        return True

    def _cyclic_members(self) -> Dict[str, str]:
        graph = {
            name: sorted({d.downstream for d in self._out_edges.get(name, [])})
            for name in self._datasets
        }
        visited: Set[str] = set()
        finish_order: List[str] = []
        for root in sorted(graph):
            if root in visited:
                continue
            visited.add(root)
            stack = [(root, iter(graph[root]))]
            while stack:
                node, it = stack[-1]
                nxt = None
                for cand in it:
                    if cand not in visited:
                        nxt = cand
                        break
                if nxt is None:
                    finish_order.append(node)
                    stack.pop()
                else:
                    visited.add(nxt)
                    stack.append((nxt, iter(graph.get(nxt, []))))
        reverse: Dict[str, List[str]] = {name: [] for name in graph}
        for src, dsts in graph.items():
            for dst in dsts:
                reverse.setdefault(dst, []).append(src)
        members: Dict[str, str] = {}
        seen: Set[str] = set()
        for node in reversed(finish_order):
            if node in seen:
                continue
            comp: List[str] = []
            seen.add(node)
            stack = [node]
            while stack:
                cur = stack.pop()
                comp.append(cur)
                for nxt in reverse.get(cur, []):
                    if nxt not in seen:
                        seen.add(nxt)
                        stack.append(nxt)
            if len(comp) > 1 or node in graph.get(node, []):
                names = sorted(comp)
                label = " -> ".join(names + [names[0]])
                for name in names:
                    members[name] = label
        return members

    def _apply_version_checks(
        self,
        tasks: Dict[Tuple[str, str], Task],
        blocked: List[BlockedTask],
        seeds: Set[Tuple[str, str]],
    ) -> None:
        materialized = {(m.dataset, m.partition) for m in self._materialized}
        mat_datasets = {m.dataset for m in self._materialized}
        blocked_keys = {(b.dataset, b.partition) for b in blocked if b.partition is not None}
        while True:
            bad = []
            for key in sorted(tasks):
                reason = self._missing_version(
                    key[0], key[1], tasks, materialized, mat_datasets, seeds, blocked_keys
                )
                if reason:
                    bad.append((key[0], key[1], reason))
            if not bad:
                return
            for ds_name, part, reason in bad:
                blocked.append(BlockedTask(ds_name, part, reason))
                blocked_keys.add((ds_name, part))
                del tasks[(ds_name, part)]

    def _missing_version(
        self,
        ds_name: str,
        part: str,
        tasks: Dict[Tuple[str, str], Task],
        materialized: Set[Tuple[str, str]],
        mat_datasets: Set[str],
        seeds: Set[Tuple[str, str]],
        blocked_keys: Set[Tuple[str, str]],
    ) -> Optional[str]:
        required: Set[Tuple[str, str]] = set()
        for dep in self._in_edges.get(ds_name, []):
            explicit = tuple(dep.rule.required(part) or ())
            if explicit:
                required.update((dep.upstream, r) for r in explicit)
            else:
                required.update(
                    t for t in self._triggers.get((ds_name, part), set()) if t[0] == dep.upstream
                )
        for up, req_part in sorted(required):
            if up not in mat_datasets:
                continue
            if (up, req_part) in blocked_keys:
                return f"missing upstream version: {up}@{req_part} (upstream blocked)"
            if (up, req_part) in materialized or (up, req_part) in tasks or (up, req_part) in seeds:
                continue
            return f"missing upstream version: {up}@{req_part}"
        return None

    def _topo_sort(self, tasks: Dict[Tuple[str, str], Task]) -> List[Task]:
        affected = {ds for ds, _ in tasks}
        indegree = {ds: 0 for ds in affected}
        adjacent: Dict[str, List[str]] = {ds: [] for ds in affected}
        for dep in self._dependencies:
            if dep.upstream in affected and dep.downstream in affected:
                adjacent[dep.upstream].append(dep.downstream)
                indegree[dep.downstream] += 1
        by_dataset: Dict[str, List[Task]] = {}
        for (ds, _), task in tasks.items():
            by_dataset.setdefault(ds, []).append(task)
        for group in by_dataset.values():
            group.sort(key=lambda t: t.partition)
        ready = sorted(ds for ds in affected if indegree[ds] == 0)
        ordered: List[Task] = []
        while ready:
            layer, ready = ready, []
            for ds in layer:
                ordered.extend(by_dataset.get(ds, []))
                for down in sorted(adjacent[ds]):
                    indegree[down] -= 1
                    if indegree[down] == 0:
                        ready.append(down)
            ready.sort()
        emitted = {(t.dataset, t.partition) for t in ordered}
        for key in sorted(tasks):
            if key not in emitted:
                ordered.append(tasks[key])
        return ordered
