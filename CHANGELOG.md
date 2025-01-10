## Next

### Fixed

### Changed

### Added

* methods to generate synthetic data that maps to each ingest method in the package
* main method to iterate through parameters and collect benchmarking information for each ingest method in the package
* visualizations to easily analyze results
* method to partition results by package version automatically

## 0.2.4

### Fixed

* Fixed delimiter in generated group ID to be consistent between group and batch processes in monopartite

## 0.2.3

### Fixed

* Fixed monopartite batching color coding process where sometimes a property value would be found in conflicting groups

### Added

* Added additional tests for monopartite batching 

## 0.2.2

### Fixed

* update how `num_groups` in ingest function is calculated internally 

## 0.2.1

### Fixed

* Fix monopartite batch assignment bug
* Fix bug in ingest where partitioning was not performed according to defined groups

## 0.2.0

### Added

* Add changelog and PR template
* Add verification function to assert Spark version is compatible (>= 3.4.0)
* Replace `final_group` column with `group` column
* Add heatmap visualization module
* Add example notebook demonstrating the heatmap module and updated README.md

## 0.1.1

* Initial release and imports update 
