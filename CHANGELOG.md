## Next

### Fixed

### Changed

* Make `num_groups` optional parameter in ingest function. Allows for improved performance. 

### Added

## 0.3.0

### Changed

* Swap heatmap visualization axes
* Add group ID to each cell in heatmap
* Heatmap scale start at 0
* Update monopartite batching algorithm
* Update ingest algorithm

### Added

* Examples demonstrating each parallel ingest method with real data

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
