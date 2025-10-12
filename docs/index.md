# Orthant

Agentic data workbench: ask questions across SQL tables, Parquet files, and LanceDB vectors. Returns ranked, reproducible answers with traces.

- **One prompt, many stores:** SQL, Parquet, LanceDB
- **Deterministic flows:** traceable, testable, CI-friendly
- **Pluggable orchestration:** Haystack, Agno, LangGraph

## What it does
- Converts natural-language questions into SQL plus vector retrieval.
- Blends relational joins with embedding search in a single flow.
- Emits typed, ranked answers with full run traces for debugging and eval.
- Ships as a CLI and HTTP API, easy to add to existing stacks.

## Built for
- Postgres or SQLite for tables, Parquet for files, LanceDB for vectors.
- Python 3.14t with uv for fast, reproducible environments.

**Next steps:** Read the [Quickstart](quickstart.md), then see [Guides → Data Sources](guides/data-sources.md).
