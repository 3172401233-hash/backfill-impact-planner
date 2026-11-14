# backfill-impact-planner
Python 数据回填影响分析库，处理分区依赖、列级血缘、增量重算和拓扑排序

上游补了数据之后，不必全链路重跑：`plan_backfill` 根据数据集依赖、列级映射、
分区谓词、已有物化分区和本次变更分区，算出真正需要重算的下游
`(dataset, partition)` 任务，按依赖拓扑分层输出；无法安全重算的链路标记为
`blocked` 并保留原因。

## 快速开始

```python
from backfill_planner import (
    ChangeEvent, Dataset, Dependency, MaterializedPartition,
    hourly_to_daily_rule, plan_backfill,
)

datasets = [
    Dataset("events_hourly", partition_keys=("p_hour",)),
    Dataset("daily_revenue", partition_keys=("p_day",)),
]
dependencies = [
    Dependency(
        "events_hourly", "daily_revenue",
        {"p_day": frozenset({"p_hour"}), "total_revenue": frozenset({"revenue"})},
        hourly_to_daily_rule(),
    ),
]
materialized = [MaterializedPartition("events_hourly", f"2026-11-13T{h:02d}") for h in range(24)]
changes = [ChangeEvent("events_hourly", ("2026-11-13T10",), ("revenue",))]

plan = plan_backfill(datasets, dependencies, materialized, changes)
# plan.tasks   -> (Task("daily_revenue", "2026-11-13", ("total_revenue",)),)
# plan.blocked -> ()
```

运行测试：`python -m unittest`

## 分区谓词（PartitionRule）

每条依赖边携带一个分区谓词，回答两个方向的问题：

- `forward(upstream_partition) -> Iterable[downstream_partition]`：一个上游分区
  变更会波及哪些下游分区。返回空（或无返回）时，该下游链路标记为 blocked，
  原因记入 `plan.blocked`。
- `required(downstream_partition) -> Iterable[upstream_partition]`：重算一个下游
  分区需要哪些上游分区齐备（已物化、在本次计划内、或本身就是本次变更分区）。
  缺任何一个即 "上游版本缺失"，任务及其下游级联 blocked。缺省实现返回空，
  此时退化为只检查实际触发该任务的上游分区。

内置谓词：

- `identity_rule()`：同粒度透传（日 -> 日）。
- `hourly_to_daily_rule()`：小时 `YYYY-MM-DDTHH` 聚合到日 `YYYY-MM-DD`，
  重算一天需要当天 24 个小时分区齐备。
- `daily_rolling_rule(window)`：跨天聚合，日 D 覆盖截至 D 的 `window` 天；
  D 变更影响 `D .. D+window-1`，重算 D 需要 `D-window+1 .. D`。

自定义谓词用 `FunctionRule(forward, required=...)` 或继承 `PartitionRule`。

## 列级传播规则

- `ChangeEvent.columns=None` 表示全分区变更，影响向下游全量扩散
  （`Task.columns=None` 表示整分区重算）。
- 普通字段变更只影响引用该字段的下游列：沿 `column_map`（下游列 -> 上游列集合）
  过滤，没有任何下游列引用时传播终止，不产生任务。
- 变更列命中数据集的 `partition_keys` 时升级为全分区变更，扩散到整个下游。
- 下游被影响的列集合继续作为下一跳传播的输入，逐跳收窄。
- `Dataset(refresh=False)` 的说明表永不进入执行计划，但影响会穿透它继续传播。
- 变更事件本身指向的 `(dataset, partition)` 视为上游已重刷，只作为传播种子，
  不会作为任务出现在计划中。

## 输出与确定性

- `Plan.tasks` 按依赖拓扑分层排序，同层按 `(dataset, partition)` 字典序，
  结果稳定：输入顺序变化、重复数据集/依赖/事件不产生不同计划
  （重复依赖的 `column_map` 自动合并，重复事件的列自动并集）。
- `Plan.blocked` 保留被阻断的链路及原因：依赖环路、引用未知分区
  （声明了 `known_partitions` 时校验）、分区函数无返回、上游版本缺失。
  被阻断任务的上游旧版本视为不可用，下游级联 blocked，避免新旧版本混跑。
- 没有任何受影响任务时返回空计划（`tasks=()`），不抛异常。
