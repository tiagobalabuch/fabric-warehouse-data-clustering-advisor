# Fabric Warehouse Advisor

A modular Python **advisory framework** for **Microsoft Fabric Warehouse**.
Each advisor module analyses a different aspect of warehouse health and
produces scored recommendations with rich reports.

**Available Advisors:**

* **Data Clustering Advisor** — assesses which tables and columns should use
  Data Clustering (scored 0–100).
* **Performance Check Advisor** — scans for data-type anti-patterns, caching
  misconfigurations, stale statistics, and V-Order issues (findings-based).
* **Security Check Advisor** — analyses permissions, roles, RLS, CLS, and
  Dynamic Data Masking configuration (findings-based).

It runs entirely inside a **Fabric Notebook**. The Microsoft Fabric Data Warehouse connector comes pre-installed in the Fabric runtime, and Query Insights is enabled by default on every warehouse. A Lakehouse is required only when the solution is installed from a wheel file stored in OneLake.

## Installation

### Option A: Install directly from PyPI (Recommended)

```python
%pip install fabric-warehouse-advisor
```

For version information, dependencies, and release notes, see the [details](https://pypi.org/project/fabric-warehouse-advisor/).

### Option B: Download Pre-Built Wheel

Download the latest `.whl` file from
[**GitHub Releases**](https://github.com/tiagobalabuch/fabric-warehouse-advisor/releases/latest),
then install it in your Fabric notebook:

```python
%pip install /lakehouse/default/Files/fabric_warehouse_advisor-1.0.3-py3-none-any.whl
```

### Option C: Build from Source

```bash
pip install build
python -m build          # produces dist/fabric_warehouse_advisor-1.0.3-py3-none-any.whl
```

### Install in Fabric

### Option A: Install directly from PyPI (Recommended)

```python
%pip install fabric-warehouse-advisor
```

### Option B: Upload the Pre-Built Wheel

Upload the `.whl` file to your Lakehouse **Files** area and use `%pip install`, or attach it via a Fabric **Environment** resource so it's pre-installed on every Spark session.

See [Getting Started](docs/getting-started.md) for detailed instructions.

## Quick Start

### Data Clustering

```python
from fabric_warehouse_advisor import DataClusteringAdvisor, DataClusteringConfig

config = DataClusteringConfig(
    warehouse_name="MyWarehouse",
)

advisor = DataClusteringAdvisor(spark, config)
result = advisor.run()

# Rich HTML report — best way to view results in a Fabric notebook
displayHTML(result.html_report)
```

### Performance Check

```python
from fabric_warehouse_advisor import PerformanceCheckAdvisor, PerformanceCheckConfig

config = PerformanceCheckConfig(
    warehouse_name="MyWarehouse",
)

advisor = PerformanceCheckAdvisor(spark, config)
result = advisor.run()

displayHTML(result.html_report)
```

### Security Check

```python
from fabric_warehouse_advisor import SecurityCheckAdvisor, SecurityCheckConfig

config = SecurityCheckConfig(
    warehouse_name="MyWarehouse",
)

advisor = SecurityCheckAdvisor(spark, config)
result = advisor.run()

displayHTML(result.html_report)
```

## How It Works

The advisor runs **7 phases** — all using T-SQL passthrough (no data
transferred to Spark):

| Phase | What it does |
|-------|-------------|
| 1. **Metadata** | Reads `sys.tables`, `sys.columns`, `sys.types` |
| 2. **Clustering** | Reads `sys.indexes` / `sys.index_columns` for current `CLUSTER BY` |
| 3. **Row Counts** | `COUNT_BIG(*)` per table; filters small tables |
| 4. **Query Patterns** | Reads `queryinsights.frequently_run_queries` |
| 5. **Predicates** | Regex extraction of WHERE-clause columns |
| 6. **Cardinality** | `APPROX_COUNT_DISTINCT()` pushed down to SQL engine |
| 7. **Scoring** | 0–100 composite score with cardinality penalties |

## Documentation

| Document | Description |
|----------|-------------|
| [Getting Started](docs/getting-started.md) | Installation, first run, working with results |
| [Advisors Overview](docs/advisors/index.md) | Comparison of all available advisors |
| **Data Clustering** | |
| [DC — How It Works](docs/advisors/data-clustering/how-it-works.md) | 7-phase pipeline deep dive |
| [DC — Configuration](docs/advisors/data-clustering/configuration.md) | Full parameter reference |
| [DC — Scoring](docs/advisors/data-clustering/scoring.md) | Scoring formula, penalties, examples |
| [DC — Reports](docs/advisors/data-clustering/reports.md) | Text, Markdown, and HTML output |
| [DC — Data Type Reference](docs/advisors/data-clustering/data-type-reference.md) | Supported types |
| **Performance Check** | |
| [PC — How It Works](docs/advisors/performance-check/how-it-works.md) | 5-phase pipeline deep dive |
| [PC — Configuration](docs/advisors/performance-check/configuration.md) | Full parameter reference |
| [PC — Check Categories](docs/advisors/performance-check/checks.md) | All checks with severity and fixes |
| [PC — Reports](docs/advisors/performance-check/reports.md) | Text, Markdown, and HTML output |
| **Security Check** | |
| [SC — How It Works](docs/advisors/security-check/how-it-works.md) | 5-phase pipeline deep dive |
| [SC — Configuration](docs/advisors/security-check/configuration.md) | Full parameter reference |
| [SC — Check Categories](docs/advisors/security-check/checks.md) | All checks with severity and fixes |
| [SC — Reports](docs/advisors/security-check/reports.md) | Text, Markdown, and HTML output |
| **Shared** | |
| [Cross-Workspace](docs/cross-workspace.md) | Analysing warehouses in other workspaces |
| [Troubleshooting](docs/troubleshooting.md) | Common issues and solutions |

## Acknowledgements

Report icons provided by [Flaticon](https://www.flaticon.com/):

- [Cyber security icons created by Freepik - Flaticon](https://www.flaticon.com/free-icons/cyber-security)
- [Performance icons created by Freepik - Flaticon](https://www.flaticon.com/free-icons/performance)
- [Graph icons created by Karacis - Flaticon](https://www.flaticon.com/free-icons/graph)

## License

MIT — see [LICENSE](LICENSE) for details.
