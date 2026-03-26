# Changelog

All notable changes to this project will be documented in this file.
This file is automatically updated by the release workflow.

## [v1.1.4](https://github.com/tiagobalabuch/fabric-warehouse-advisor/releases/tag/v1.1.4) - 2026-03-26

### Features

- Add max_parallel_tables configuration for cardinality estimation ([#28](https://github.com/tiagobalabuch/fabric-warehouse-advisor/pull/28))
- Implement best practices section and enhance report UX ([#26](https://github.com/tiagobalabuch/fabric-warehouse-advisor/pull/26))
- Add OneLake settings and security checks ([#25](https://github.com/tiagobalabuch/fabric-warehouse-advisor/pull/25))
- Refactor phase management and enhance output for performance and security checks ([#24](https://github.com/tiagobalabuch/fabric-warehouse-advisor/pull/24))
- Fix/report bugs ([#22](https://github.com/tiagobalabuch/fabric-warehouse-advisor/pull/22))
- Enhance documentation and logging for advisors ([#21](https://github.com/tiagobalabuch/fabric-warehouse-advisor/pull/21))

### Bug Fixes

- Update version to 1.1.4 and add beta usage note for SQL Pools Configuration API ([#30](https://github.com/tiagobalabuch/fabric-warehouse-advisor/pull/30))
- Implement schema filtering in data clustering and security checks ([#23](https://github.com/tiagobalabuch/fabric-warehouse-advisor/pull/23))

### Documentation

- Enhance Security Check Advisor documentation and functionality ([#29](https://github.com/tiagobalabuch/fabric-warehouse-advisor/pull/29))

### Other Changes

- fix: Update MkDocs installation to include mkdocs-glightbox
- fix: Add environment variable FORCE_JAVASCRIPT_ACTIONS_TO_NODE24 to workflows
- Integrate REST client for workspace metadata and enhance performance advisor ([#27](https://github.com/tiagobalabuch/fabric-warehouse-advisor/pull/27))

## [v1.1.3](https://github.com/tiagobalabuch/fabric-warehouse-advisor/releases/tag/v1.1.3) - 2026-03-17

### Features

- Add HTML report template and enhance SQL security checks ([#18](https://github.com/tiagobalabuch/fabric-warehouse-advisor/pull/18))
- Add comprehensive security checks for Microsoft Fabric ([#17](https://github.com/tiagobalabuch/fabric-warehouse-advisor/pull/17))
- Add Security Check Advisor with configuration, findings, and reporting ([#16](https://github.com/tiagobalabuch/fabric-warehouse-advisor/pull/16))
- Update issue templates ([#15](https://github.com/tiagobalabuch/fabric-warehouse-advisor/pull/15))

### Chore

- Update CHANGELOG.md for v1.0.7 ([#14](https://github.com/tiagobalabuch/fabric-warehouse-advisor/pull/14))

## [v1.0.7](https://github.com/tiagobalabuch/fabric-warehouse-advisor/releases/tag/v1.0.7) - 2026-03-11

### Features

- Update version to 0.3.0 and add timing for phases in DataClusteringAdvisor and row counting in warehouse_reader ([#4](https://github.com/tiagobalabuch/fabric-warehouse-advisor/pull/4))

### Bug Fixes

- Refactor Performance Check Advisor and update version to 1.0.3 ([#10](https://github.com/tiagobalabuch/fabric-warehouse-advisor/pull/10))
- Add Fabric Warehouse Advisor core modules and performance checks ([#7](https://github.com/tiagobalabuch/fabric-warehouse-advisor/pull/7))

### Documentation

- Update installation instructions and refactor Data Clustering Advisor ([#12](https://github.com/tiagobalabuch/fabric-warehouse-advisor/pull/12))
- Enhance documentation for advisors ([#9](https://github.com/tiagobalabuch/fabric-warehouse-advisor/pull/9))
- Improve documentation clarity and detail ([#5](https://github.com/tiagobalabuch/fabric-warehouse-advisor/pull/5))
- Update version to 0.3.0, enhance release workflow with changelog generation, and remove hybrid predicate extraction strategy ([#2](https://github.com/tiagobalabuch/fabric-warehouse-advisor/pull/2))

### Chore

- Update CHANGELOG.md for v1.0.3 ([#11](https://github.com/tiagobalabuch/fabric-warehouse-advisor/pull/11))
- Update CHANGELOG.md for v1.0.0 ([#8](https://github.com/tiagobalabuch/fabric-warehouse-advisor/pull/8))
- Update CHANGELOG.md for v0.3.0 ([#6](https://github.com/tiagobalabuch/fabric-warehouse-advisor/pull/6))
- Update CHANGELOG.md for v0.2.1 ([#3](https://github.com/tiagobalabuch/fabric-warehouse-advisor/pull/3))

### Other Changes

- Bump version to 1.0.7 in pyproject.toml
- Update installation instructions to remove version pinning and correct project URLs
- Base changelog branch on master to avoid workflows permission issue
- Use force push for changelog branch
- Handle existing changelog branch in release workflow
- Add pull-requests write permission to release workflow
- Enhance release workflow to commit CHANGELOG.md and create a pull request for automated updates
- Fix release workflow to push to default branch and update predicate parser documentation. Fixing the write version number
- Bump version to 0.2.0 in pyproject.toml
- Implement hybrid predicate extraction strategy using execution plans
- Add MkDocs configuration and documentation files for project deployment
- Renamed data clustering functions
- Bump version to 0.3.0
- Refactor get_table_row_counts function to remove redundant filtering logic
- Update documentation and function names to reflect Microsoft Fabric Data Warehouse terminology
- Add in-repo documentation and GitHub Actions release workflow ([#1](https://github.com/tiagobalabuch/fabric-warehouse-advisor/pull/1))

