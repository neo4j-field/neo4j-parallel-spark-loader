## Next

### Fixed

* `ingest_spark_dataframe` no longer silently drops rows whose `batch` is `null` (rows with a `null` node id or partition value that could not be assigned to a group). It now counts them in the same pass that collects the batch values and raises a `ValueError` before writing anything. Pass `on_null_batch="skip"` to warn and ingest the remaining rows instead.
* Monopartite grouping now assigns a `null` `group` when either the source or target id is `null`. Previously `least`/`greatest` skipped the `null` side, so such rows landed in a real self-loop group (for example `"3 -- 3"`) and were sent to Neo4j.

### Changed

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
