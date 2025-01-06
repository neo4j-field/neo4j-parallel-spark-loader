#!/bin/bash 
poetry_version=$(poetry version -s)
sed -i.bak -e 's/## Next/## Next\n\n### Fixed\n\n### Changed\n\n### Added\n\n## '$poetry_version'/' CHANGELOG.md && rm CHANGELOG.md.bak 