# Add opt-in hash-based grouping strategy

## Problem

`group_and_batch_spark_dataframe` in all three scenario modules (`bipartite`, `monopartite`,
`predefined_components`) funnels through `utils/grouping.py::create_value_groupings`, which:

1. `groupBy(node_id).count()`
2. `.collect()`s every distinct node ID + count to the driver
3. runs a single-threaded Python greedy bin-packing loop over that list (a linear scan of all
   buckets per iteration)
4. `spark.createDataFrame(list_of_dicts)` to ship the ID -> group map back to Spark
5. joins the full DataFrame against that map (twice, for `bipartite`/`monopartite`)

Cost scales with the number of distinct node IDs and is entirely serial on the driver. On a
300M-row dataset this took ~30 minutes; at 2B rows it will also risk OOMing the driver, since
every distinct ID and its count must fit in driver memory as Python objects.

## Design

Added `strategy: Literal["greedy", "hash"] = "greedy"` to `create_node_groupings` and
`group_and_batch_spark_dataframe` in all three scenario modules, and threaded it through
`build_relationship`. **Default behavior is unchanged** — `strategy` defaults to `"greedy"`
everywhere, and the existing greedy code path is untouched.

For `strategy="hash"`, group assignment is computed entirely in Spark, with no `count()`, no
`collect()`, and no join:

* `neo4j_parallel_spark_loader/utils/hash_grouping.py` adds `hash_group_column(column_name,
  num_groups)`, a Column expression equal to `pmod(hash(col(column_name)), num_groups)`, cast to
  the same `LongType` the greedy path produces (so downstream arithmetic like
  `(source_group + target_group) % num_colors` keeps working unchanged).
* **Bipartite**: `source_group`/`target_group` are `hash_group_column(source_col, num_groups)` /
  `hash_group_column(target_col, num_groups)` respectively; `group` is still
  `"{source_group} --> {target_group}"`.
* **Monopartite**: the *identical* expression is applied to `source_col` and `target_col`
  independently, so a node ID lands in the same group whether it appears as a source or a
  target (required for the deadlock-free invariant). `group` is still
  `"{min} -- {max}"`.
* **Predefined components**: `group` is `hash_group_column(partition_col, num_groups)` directly
  (an int, matching the greedy path's output type).
* **Null handling**: Spark's `hash()` returns a constant, non-null value for `null` input. The
  greedy path currently produces a `null` group for a `null` node ID as a side effect of its
  left join (a `null` key never matches the grouping map, since SQL `null = null` is not true).
  To keep behavior consistent between strategies, `hash_group_column` explicitly maps `null`
  input to a `null` group. This is documented in the function's docstring.

### Batching

The bipartite and monopartite `create_ingest_batches_from_groups` functions used to run
`distinct().count()` on the group columns to learn how many groups exist, which triggers a full
pass over the data. Both now accept an optional `known_group_count: Optional[int] = None`
parameter; when the hash strategy is used, `group_and_batch_spark_dataframe` passes `num_groups`
through as `known_group_count`, skipping those counts entirely. This is safe because hash groups
are guaranteed to be in `[0, num_groups)`, and coloring a complete graph with `num_groups`
vertices remains deadlock-free even when some groups turn out to be empty (verified in the new
monopartite hash-strategy tests). `predefined_components` batching was already O(1) (`batch` is
always `0`) and needed no change.

### Not included: heavy-hitter handling

A `heavy_hitter_limit` parameter (count node IDs, greedily bin-pack only the top-N by count,
broadcast-join that small map, and `coalesce` it onto the hashed group) was considered but left
out to keep this PR focused. It's called out as a follow-up below.

## Backward compatibility

* All new parameters default to preserving current behavior (`strategy="greedy"`,
  `known_group_count=None`).
* No changes to `ingest_spark_dataframe` in this PR.
* All existing unit tests pass unmodified, confirming greedy-path output is unchanged.

## Test coverage

Added unit tests for `strategy="hash"` in each scenario module
(`tests/unit/{bipartite,monopartite,predefined_components}/test_*_hash_strategy.py`) and for the
new `hash_group_column` helper (`tests/unit/utils/test_hash_grouping.py`), covering:

* every row receives a non-null `group`/`batch` (for non-null input IDs)
* `group`/`source_group`/`target_group` values fall in `[0, num_groups)`
* the deadlock invariant: within each batch, each source ID appears in exactly one group
  (bipartite, predefined components); for monopartite, each node ID (as source or target)
  appears in exactly one group per batch
* monopartite: the same node ID hashes to the same group whether it's a source or a target
* same input produces identical assignments across two calls (determinism)
* `null` input maps to a `null` group
* `known_group_count` produces identical output to the counted path (bipartite, monopartite
  batching)
* `strategy="greedy"` (the default, exercised by the full pre-existing test suite) is unchanged

Also ran the full unit suite (`pytest tests/unit`) and `ruff check`/`ruff format --check`.
Integration tests (`tests/integration`) were **not** run — they require a live Neo4j instance in
Docker.

## Follow-ups (not in this PR)

1. **`ingest_spark_dataframe` laziness.** The grouped/batched DataFrame returned by
   `group_and_batch_spark_dataframe` is lazy, and its `groupBy`/join chain re-executes on every
   downstream action: the batching functions' `distinct().count()` (skipped only when
   `known_group_count` is passed), `ingest_spark_dataframe`'s `collect_set("batch")` +
   `distinct().count()` on `group`, and once again per `batch` filter during the actual write
   loop. On a 2B-row dataset that's the same expensive computation repeated many times over.
   Recommend materializing the grouped/batched result to Delta (partitioned by `batch`) before
   calling `ingest_spark_dataframe`, so each batch read is a cheap partition scan rather than a
   recomputation. Proposed as a separate follow-up PR — it touches `ingest_spark_dataframe`'s
   contract and deserves its own review.
2. **Heavy-hitter handling for the hash strategy** (`heavy_hitter_limit`): count node IDs, greedily
   assign only the top-N by count to groups, broadcast-join that small map, and `coalesce` it
   over the hashed group. Would let the hash strategy handle a small number of extreme
   supernodes without a full driver collect. Skipped here to keep this PR scoped; worth adding
   if benchmarking below shows supernodes are a problem in practice.

## Suggested benchmark plan

1. On the 300M-row dataset used for the original greedy timing (~30 min), first run a
   top-50-by-count query on the source/target ID columns to check the degree distribution. If
   the top IDs' counts are within a small multiple of the median, `hash` should perform well as
   -is; if a handful of IDs dominate the row count (classic power-law/supernode shape), that's a
   signal `heavy_hitter_limit` (follow-up #2 above) is worth prioritizing before relying on
   `hash` in production.
2. Run `group_and_batch_spark_dataframe(..., strategy="greedy")` vs. `strategy="hash"` on the
   same dataset and compare: wall-clock time, driver memory high-water mark, and (via the
   heatmap visualization module) how balanced the resulting batches are.
3. Re-run at a larger scale (approaching the 2B-row target) with `strategy="hash"` only, since
   `greedy` is expected to become impractical there.
