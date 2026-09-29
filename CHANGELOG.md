## Unreleased

### Added

* `build_relationship` accepts `materialize`. `"persist"` caches the input with `DataFrame.persist()` for the duration of the load; any other string is a path the input is written to once as Parquet and read back from. Either way the input's lineage is computed exactly once, instead of once for grouping and once per batch.
* `IngestPlan`, exported from the package root. `group_and_batch_spark_dataframe(..., return_plan=True)` in every scenario returns `(DataFrame, IngestPlan)`, and `ingest_spark_dataframe(..., plan=plan)` uses it to skip its pass over the DataFrame that finds the `(batch, group)` pairs and counts rows with a `null` batch or group. With a plan, `on_null_batch="raise"` fails before anything is written, including the checkpoint.

### Fixed

* `build_relationship` is now exported from the package root, so `from neo4j_parallel_spark_loader import build_relationship` works as documented in the README. `neo4j_parallel_spark_loader.utils.build_relationship` now uses relative imports to avoid a circular import with the root package.
* Greedy grouping no longer leaves rows with a `null` group when their node id or partition value is not `null`. It used to collect the raw values to the driver and join the resulting groups back by value, so a value that did not survive the round trip unchanged matched nothing. That happened to strings containing invalid UTF-8, which Python decodes with replacement characters, and to any value produced by a non-deterministic input whose re-evaluation differed from the counting pass. `ingest_spark_dataframe` then rejected those rows with "row(s) have a null `batch` or `group`". Greedy grouping now counts and joins on `xxhash64` of the value computed inside Spark, so values never leave the JVM, and only `(key, count)` longs are collected to the driver. A non-null key missing from the mapping gets the fallback group `pmod(key, group_count)`, so every row sharing a value still lands in the same group. Hash collisions only put extra values in the same group, which cannot break the deadlock-free guarantee.
* Bipartite and monopartite greedy `group_and_batch_spark_dataframe` pass the number of groups used by the grouping step to batching, instead of running `distinct().count()` passes over `source_group`/`target_group`. This removes two (bipartite) or one (monopartite) evaluations of the input, and keeps the batch schedule valid even if a non-deterministic input leaves some groups empty.

### Changed

* `build_relationship` evaluates the input once for grouping and once per batch, and nothing else. Grouping now builds an `IngestPlan` (the batches, the groups in each, and the total and `null` row counts) from its counting pass, and `build_relationship` passes it to `ingest_spark_dataframe`, replacing the separate `df.count()` and ingest's schedule pass. Bipartite greedy grouping also counts source and target ids in a single pass instead of two. A predefined components load now evaluates its input 2 times instead of 4 (greedy) or 3 (hash); a bipartite load with 3 batches 4 times instead of 7 (greedy) or 5 (hash).
* Monopartite greedy grouping hashes the source and target ids as their common type, so the same id in an `int` column and a `long` column gets the same group, as the previous value join did.
* `build_relationship` groups before deciding between the serial and parallel paths, since the row count now comes from grouping. Inputs at or below `max_serial` still load serially, after a grouping pass over the (small) input.
* Removed the internal `create_value_groupings` helper from `neo4j_parallel_spark_loader.utils.grouping`, replaced by `create_key_groupings` and `apply_key_groupings`.

## v0.6.0 (2026-09-19)

### Fixed

* Bump `fonttools`, `idna`, `pillow`, `pygments`, `pytest`, `python-dotenv`, `requests`, `setuptools`, `tornado`, and `urllib3` to patched versions in `poetry.lock`, resolving 45 open Dependabot alerts. All are transitive dependencies of the `dev`/`benchmarking` Poetry groups (via `ipykernel`, `seaborn`, `requests`, `neo4j`); none are runtime dependencies of the published package.
* `ingest_spark_dataframe` now writes every group in a batch from its own Spark partition. Previously each batch was `repartition(num_groups, "group")`, and because Spark places rows by hashing the column, roughly a third of the partitions were empty while others held two or three groups, so a batch ran at the speed of its most crowded partition. Each group is now mapped to a long key known to hash to a distinct partition, so a batch with `k` groups runs as exactly `k` parallel writers.
* `build_relationship` counts the DataFrame once instead of twice.
* `ingest_spark_dataframe` no longer silently drops rows whose `batch` is `null` (rows with a `null` node id or partition value that could not be assigned to a group). It now counts them in the same pass that collects the batch schedule and raises a `ValueError` before writing anything. Pass `on_null_batch="skip"` to warn and ingest the remaining rows instead.
* Monopartite grouping now assigns a `null` `group` when either the source or target id is `null`. Previously `least`/`greatest` skipped the `null` side, so such rows landed in a real self-loop group (for example `"3 -- 3"`) and were sent to Neo4j.

### Changed

* `ingest_spark_dataframe` accepts `checkpoint_path`. When set, the grouped DataFrame is written once as Parquet partitioned by `batch` and each batch is read back from there, so the input plan is not recomputed once per batch. The checkpoint can also be used to resume a failed load. `build_relationship` passes `checkpoint_path` and `on_null_batch` through.
* `ingest_spark_dataframe` reports progress per batch through the `neo4j_parallel_spark_loader.utils.ingest` logger.
* The `num_groups` parameter of `ingest_spark_dataframe` is deprecated and ignored. The partition count for each batch is now the number of groups observed in that batch.
* `ingest_spark_dataframe` treats a `null` `group` the same as a `null` `batch`. In the predefined components scenario rows with a `null` partition value have `batch` 0 but a `null` group, and were previously written alongside real groups.
* Monopartite `create_node_groupings` with `strategy="hash"` raises a `TypeError` when the source and target id columns have different data types. Spark's `hash()` depends on the type, so the same id would otherwise land in different groups and break the deadlock-free guarantee.
* Bipartite and monopartite `create_ingest_batches_from_groups` functions accept an optional `known_group_count` parameter to skip a `distinct().count()` pass when the number of groups is already known (used by the "hash" grouping strategy).
* Cap supported Python to `>=3.10,<3.14`. PySpark 3.5.4's cloudpickle-based closure serialization is not yet compatible with CPython 3.14.

### Added

* `ingest_spark_dataframe` and `build_relationship` accept `sort_columns`. Each group's rows are sorted by these columns, typically the source node id, within their partition before being written. Hash grouping otherwise leaves rows in random order, so consecutive relationships touch unrelated node records and dirty a fresh page almost every row; sorting keeps Neo4j checkpoints short under a sustained write load. Grouping and batching are unchanged, so the deadlock-free guarantee is unaffected.
* Opt-in `strategy="hash"` grouping strategy for `bipartite`, `monopartite`, and `predefined_components` `group_and_batch_spark_dataframe`/`create_node_groupings`, and for `build_relationship`. Computes group assignments entirely in Spark using `hash(id) % num_groups`, with no `collect()` to the driver and no join back against the source DataFrame. Scales to very large distinct-ID counts at the cost of not balancing group sizes; the default `strategy="greedy"` is unchanged.

## v0.5.2

### Changed
* `verify_spark` function now warns instead of raising assertion error if not using Spark v3.4.0 or greater

## v0.5.1

### Fixed
* fix `build_relationship` function where serial relationship loading was loading in parallel

## v0.5.0

### Added

* `build_relationship` function to simplify creating relationships in parallel

## v0.4.0

### Changed

* For monopartite batching assign self loop relationships for two node groups to the same relationship group. Allows for improved efficiency for most real-world monopartite graphs.

### Added

* Benchmarking module including:
  * Methods to generate synthetic data that maps to each ingest method in the package
  * Generate_benchmarks method to iterate through parameters and collect benchmarking information for each ingest method in the package
  * Visualizations to easily analyze results
  * Method to partition results by package version automatically
  * Methods to retrieve and format real data for benchmarking scenarios

## v0.3.1

### Changed

* Make `num_groups` optional parameter in ingest function. Allows for improved performance. 

## v0.3.0

### Changed

* Swap heatmap visualization axes
* Add group ID to each cell in heatmap
* Heatmap scale start at 0
* Update monopartite batching algorithm
* Update ingest algorithm

### Added

* Examples demonstrating each parallel ingest method with real data

## v0.2.4

### Fixed

* Fixed delimiter in generated group ID to be consistent between group and batch processes in monopartite

## v0.2.3

### Fixed

* Fixed monopartite batching color coding process where sometimes a property value would be found in conflicting groups

### Added

* Added additional tests for monopartite batching 

## v0.2.2

### Fixed

* update how `num_groups` in ingest function is calculated internally 

## v0.2.1

### Fixed

* Fix monopartite batch assignment bug
* Fix bug in ingest where partitioning was not performed according to defined groups

## v0.2.0

### Added

* Add changelog and PR template
* Add verification function to assert Spark version is compatible (>= 3.4.0)
* Replace `final_group` column with `group` column
* Add heatmap visualization module
* Add example notebook demonstrating the heatmap module and updated README.md

## v0.1.1

* Initial release and imports update 
