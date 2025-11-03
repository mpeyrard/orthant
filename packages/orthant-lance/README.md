orthant-lance
==============

LanceDB integration for Orthant: schema helpers and storage implementation.

Install (editable, dev):

```bash
. .venv/bin/activate
python -m pip install -e packages/orthant-lance
```

Quick usage:

```python
from orthant.lance import LanceDocumentStorage
storage = LanceDocumentStorage(uri='lance://db', table_name='docs', embedding_dim=1024)
await storage.store_async(document)
```

Run tests:

```bash
pytest packages/orthant-lance -q
```
