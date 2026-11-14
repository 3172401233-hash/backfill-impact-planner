import unittest

from backfill_planner import (
    ChangeEvent,
    Dataset,
    Dependency,
    FunctionRule,
    MaterializedPartition,
    Task,
    daily_rolling_rule,
    hourly_to_daily_rule,
    identity_rule,
    plan_backfill,
)

DAYS = ["2026-11-12", "2026-11-13", "2026-11-14"]
HOURS = [f"{day}T{h:02d}" for day in DAYS for h in range(24)]


def build_datasets():
    return [
        Dataset("events_hourly", partition_keys=("p_hour",)),
        Dataset("daily_revenue", partition_keys=("p_day",)),
        Dataset("daily_users", partition_keys=("p_day",)),
        Dataset("rolling_2day", partition_keys=("p_day",)),
        Dataset("report_note", partition_keys=("p_day",), refresh=False),
        Dataset("dim_country"),
    ]


def build_dependencies():
    return [
        Dependency(
            "events_hourly",
            "daily_revenue",
            {
                "p_day": frozenset({"p_hour"}),
                "total_revenue": frozenset({"revenue"}),
                "total_clicks": frozenset({"clicks"}),
            },
            hourly_to_daily_rule(),
        ),
        Dependency(
            "events_hourly",
            "daily_users",
            {
                "p_day": frozenset({"p_hour"}),
                "uv": frozenset({"user_id"}),
            },
            hourly_to_daily_rule(),
        ),
        Dependency(
            "daily_revenue",
            "rolling_2day",
            {
                "p_day": frozenset({"p_day"}),
                "rev_2d": frozenset({"total_revenue"}),
            },
            daily_rolling_rule(window=2),
        ),
        Dependency(
            "daily_revenue",
            "report_note",
            {
                "p_day": frozenset({"p_day"}),
                "note": frozenset({"total_revenue"}),
            },
            identity_rule(),
        ),
    ]


def build_materialized():
    parts = [MaterializedPartition("events_hourly", h) for h in HOURS]
    parts += [MaterializedPartition("daily_revenue", d) for d in DAYS]
    parts += [MaterializedPartition("daily_users", d) for d in DAYS]
    parts += [MaterializedPartition("rolling_2day", d) for d in DAYS[:2]]
    parts += [MaterializedPartition("dim_country", "all")]
    return parts


def plan_for(changes, materialized=None):
    return plan_backfill(
        build_datasets(),
        build_dependencies(),
        build_materialized() if materialized is None else materialized,
        changes,
    )


class ColumnPropagationTest(unittest.TestCase):
    def test_single_column_change_only_hits_referencing_columns(self):
        plan = plan_for([ChangeEvent("events_hourly", ("2026-11-13T10",), ("revenue",))])
        self.assertEqual(plan.blocked, ())
        self.assertEqual(
            plan.tasks,
            (
                Task("daily_revenue", "2026-11-13", ("total_revenue",)),
                Task("rolling_2day", "2026-11-13", ("rev_2d",)),
                Task("rolling_2day", "2026-11-14", ("rev_2d",)),
            ),
        )

    def test_unreferenced_column_change_yields_empty_plan(self):
        plan = plan_for([ChangeEvent("events_hourly", ("2026-11-13T10",), ("debug_flag",))])
        self.assertEqual(plan.tasks, ())
        self.assertEqual(plan.blocked, ())

    def test_full_partition_change_spreads_to_all_downstream(self):
        plan = plan_for([ChangeEvent("events_hourly", ("2026-11-13T10",))])
        self.assertEqual(plan.blocked, ())
        self.assertEqual(
            plan.tasks,
            (
                Task("daily_revenue", "2026-11-13"),
                Task("daily_users", "2026-11-13"),
                Task("rolling_2day", "2026-11-13"),
                Task("rolling_2day", "2026-11-14"),
            ),
        )

    def test_partition_key_change_escalates_to_full(self):
        plan = plan_for([ChangeEvent("events_hourly", ("2026-11-13T10",), ("p_hour",))])
        full = plan_for([ChangeEvent("events_hourly", ("2026-11-13T10",))])
        self.assertEqual(plan, full)

    def test_doc_table_never_enters_plan(self):
        plan = plan_for([ChangeEvent("events_hourly", ("2026-11-13T10",))])
        self.assertNotIn("report_note", {t.dataset for t in plan.tasks})


class GraphShapeTest(unittest.TestCase):
    def test_common_ancestor_fans_out_to_multiple_downstreams(self):
        changes = [ChangeEvent("events_hourly", ("2026-11-13T10",), ("revenue", "user_id"))]
        plan = plan_for(changes)
        by_dataset = {}
        for task in plan.tasks:
            by_dataset.setdefault(task.dataset, []).append(task)
        self.assertEqual(by_dataset["daily_revenue"], [Task("daily_revenue", "2026-11-13", ("total_revenue",))])
        self.assertEqual(by_dataset["daily_users"], [Task("daily_users", "2026-11-13", ("uv",))])
        self.assertEqual(len(by_dataset["rolling_2day"]), 2)

    def test_topo_order_layers_then_stable_names(self):
        changes = [
            ChangeEvent("events_hourly", ("2026-11-13T03", "2026-11-13T07")),
        ]
        plan = plan_for(changes)
        self.assertEqual(
            [t.dataset for t in plan.tasks],
            ["daily_revenue", "daily_users", "rolling_2day", "rolling_2day"],
        )

    def test_unrelated_branch_is_untouched(self):
        plan = plan_for([ChangeEvent("dim_country", ("all",))])
        self.assertEqual(plan.tasks, ())
        self.assertEqual(plan.blocked, ())

    def test_determinism_against_order_and_duplicates(self):
        changes = [
            ChangeEvent("events_hourly", ("2026-11-13T10",), ("revenue",)),
            ChangeEvent("events_hourly", ("2026-11-13T11",), ("clicks",)),
        ]
        baseline = plan_for(changes)
        shuffled = plan_backfill(
            list(reversed(build_datasets())) + build_datasets()[:2],
            list(reversed(build_dependencies())) + build_dependencies()[:1],
            list(reversed(build_materialized())) + build_materialized()[:3],
            list(reversed(changes)) + changes,
        )
        self.assertEqual(baseline, shuffled)


class BlockedTest(unittest.TestCase):
    def test_materialized_gap_blocks_chain_with_reason(self):
        materialized = [
            m for m in build_materialized()
            if not (m.dataset == "events_hourly" and m.partition == "2026-11-13T05")
        ]
        plan = plan_for([ChangeEvent("events_hourly", ("2026-11-13T10",))], materialized)
        self.assertEqual(plan.tasks, ())
        reasons = {b.dataset: b.reason for b in plan.blocked}
        self.assertIn("events_hourly@2026-11-13T05", reasons["daily_revenue"])
        self.assertIn("missing upstream version", reasons["daily_users"])
        self.assertIn("daily_revenue@2026-11-13", reasons["rolling_2day"])

    def test_dependency_cycle_is_blocked(self):
        datasets = [Dataset("a"), Dataset("b")]
        deps = [
            Dependency("a", "b", {"x": frozenset({"y"})}, identity_rule()),
            Dependency("b", "a", {"y": frozenset({"x"})}, identity_rule()),
        ]
        plan = plan_backfill(datasets, deps, [], [ChangeEvent("a", ("p1",))])
        self.assertEqual(plan.tasks, ())
        self.assertEqual(len(plan.blocked), 2)
        for blocked in plan.blocked:
            self.assertIn("dependency cycle detected", blocked.reason)
            self.assertIn("a -> b -> a", blocked.reason)

    def test_unknown_partition_reference_is_blocked(self):
        datasets = [
            Dataset("src", known_partitions=frozenset({"2026-11-13"})),
            Dataset("dst"),
        ]
        deps = [Dependency("src", "dst", {"x": frozenset({"y"})}, identity_rule())]
        plan = plan_backfill(datasets, deps, [], [ChangeEvent("src", ("2026-11-99",))])
        self.assertEqual(plan.tasks, ())
        self.assertEqual(len(plan.blocked), 1)
        self.assertIn("unknown partition '2026-11-99'", plan.blocked[0].reason)

    def test_unknown_partition_from_rule_output_is_blocked(self):
        datasets = [
            Dataset("src"),
            Dataset("dst", known_partitions=frozenset({"ok"})),
        ]
        deps = [Dependency("src", "dst", {"x": frozenset({"y"})}, identity_rule())]
        plan = plan_backfill(datasets, deps, [], [ChangeEvent("src", ("bogus",))])
        self.assertEqual(plan.tasks, ())
        self.assertEqual(len(plan.blocked), 1)
        self.assertIn("unknown partition 'bogus' for dataset 'dst'", plan.blocked[0].reason)

    def test_partition_rule_without_output_is_blocked(self):
        datasets = [Dataset("src"), Dataset("dst")]
        deps = [
            Dependency("src", "dst", {"x": frozenset({"y"})}, FunctionRule(lambda p: ()))
        ]
        plan = plan_backfill(datasets, deps, [], [ChangeEvent("src", ("p1",))])
        self.assertEqual(plan.tasks, ())
        self.assertEqual(len(plan.blocked), 1)
        self.assertIn("returned no partitions", plan.blocked[0].reason)

    def test_missing_upstream_version_is_blocked(self):
        materialized = [
            m for m in build_materialized()
            if not (m.dataset == "daily_revenue" and m.partition == "2026-11-12")
        ]
        changes = [ChangeEvent("events_hourly", ("2026-11-13T10",), ("revenue",))]
        plan = plan_for(changes, materialized)
        blocked = {(b.dataset, b.partition): b.reason for b in plan.blocked}
        self.assertIn(("rolling_2day", "2026-11-13"), blocked)
        self.assertIn("daily_revenue@2026-11-12", blocked[("rolling_2day", "2026-11-13")])
        self.assertIn(Task("daily_revenue", "2026-11-13", ("total_revenue",)), plan.tasks)


class EmptyPlanTest(unittest.TestCase):
    def test_no_changes_returns_empty_plan(self):
        plan = plan_for([])
        self.assertEqual(plan.tasks, ())
        self.assertEqual(plan.blocked, ())


if __name__ == "__main__":
    unittest.main()
