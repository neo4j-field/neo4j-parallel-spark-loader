## Next

### Fixed

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

### Added

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
