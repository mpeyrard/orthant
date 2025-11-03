orthant-embedding
==================

Embedding interfaces and helpers for Orthant.

Install (editable, dev):

```bash
. .venv/bin/activate
python -m pip install -e packages/orthant-embedding
```

Quick usage:

```python
from orthant.embedding import ChunkingStrategy
# implement a ChunkingStrategy and use it to split documents for embedding
```

Run tests (if any):

```bash
pytest packages/orthant-embedding -q
```

Notes:
- Contains the `EmbeddingClient` protocol and `ChunkingStrategy` protocol.
- Providers (e.g., Mistral) implement `EmbeddingClient` in separate packages.
