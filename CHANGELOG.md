## Next

### Fixed

* Fixed monopartite batching color coding process where sometimes a property value would be found in conflicting groups

### Changed

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
