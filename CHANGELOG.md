## Next

### Fixed

### Changed

### Added

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
