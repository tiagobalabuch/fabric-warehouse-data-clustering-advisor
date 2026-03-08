# Fabric Warehouse Data Clustering Advisor

Welcome! This project helps you answer a deceptively simple question:
**which columns in my Fabric Warehouse should I cluster?**

Data Clustering is one of the most impactful performance levers in
Microsoft Fabric Warehouse — it controls how data is physically organized
on disk, which directly affects query speed and resource consumption.
But choosing the *right* columns to cluster on isn't always obvious.
That's where this advisor comes in.

## What does it do?

The **Fabric Warehouse Data Clustering Advisor** is a PySpark library that
automatically analyses your warehouse and produces actionable, scored
recommendations for Data Clustering — no guesswork required.

It looks at your **actual query patterns** (via Query Insights), combines
them with **table metadata** and **column cardinality estimates**, and
scores every candidate column from 0 to 100. You get a clear report
telling you exactly what to cluster and why.

Everything runs inside a **Fabric Spark notebook** — no external tools,
no Lakehouse attachment, and no data leaves your environment.

## Why use it?

- **Data-driven decisions** — recommendations are based on your real
  workload, not rules of thumb
- **Zero setup** — Query Insights is enabled by default on every Fabric
  Warehouse; just install the library and run
- **Non-invasive** — read-only analysis via T-SQL passthrough; nothing is
  modified in your warehouse
- **Rich output** — interactive HTML reports, Markdown, plain text, and a
  Spark DataFrame you can persist to Delta for tracking over time
- **Cross-workspace support** — analyse warehouses in other Fabric
  workspaces from a single notebook

## Get Started

Ready to try it? Head over to [Getting Started](getting-started.md) for
installation instructions and a quick-start example.

## Documentation

| Document | Description |
|----------|-------------|
| [Getting Started](getting-started.md) | Installation, first run, working with results |
| [Configuration](configuration.md) | Full parameter reference with defaults |
| [How It Works](how-it-works.md) | Deep dive into the 7-phase pipeline |
| [Scoring](scoring.md) | Scoring formula, cardinality penalties, worked examples |
| [Reports](reports.md) | Text, Markdown, and HTML report formats |
| [Cross-Workspace](cross-workspace.md) | Analysing warehouses in other workspaces |
| [Data Type Reference](data-type-reference.md) | Supported types and limitations |
| [Troubleshooting](troubleshooting.md) | Common issues and solutions |

## License

MIT — see [LICENSE](https://github.com/tiagobalabuch/fabric-warehouse-data-clustering-advisor/blob/master/LICENSE) for details.
