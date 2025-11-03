orthant-mistral
================

Mistral embedding client integration for Orthant.

Install (editable, dev):

```bash
. .venv/bin/activate
python -m pip install -e packages/orthant-mistral
```

Quick usage:

```python
from orthant.mistral import MistralEmbeddingClient
client = MistralEmbeddingClient(api_key="...")
emb = await client.encode_document_async("Hello world")
```

Run tests:

```bash
pytest packages/orthant-mistral -q
```

Notes:
- This package depends on `mistralai` and `httpx`.
- A system test exists that reads `MISTRAL_API_KEY` from a .env at repo root.
orthant-documents
==================

Document abstractions and utilities for Orthant.

Install (editable, dev):

```bash
. .venv/bin/activate
python -m pip install -e packages/orthant-documents[test]
```

Quick usage:

```python
from orthant.documents import DefaultContentLoader
loader = DefaultContentLoader()
text = loader.load_text('memory://.../file.txt')
```

Run tests:

```bash
pytest packages/orthant-documents -q
```

Notes:
- The `DefaultContentLoader` uses `fsspec` and supports `memory://`, `zip://`, and other fsspec URIs.
- Document models are Pydantic based for validation and serialization.
