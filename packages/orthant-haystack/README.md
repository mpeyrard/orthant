orthant-haystack
=================

Adapters and utilities that integrate Haystack components with Orthant.

Install (editable, dev):

```bash
. .venv/bin/activate
python -m pip install -e packages/orthant-haystack
```

Quick usage:

```python
from orthant.haystack import HaystackChunkingStrategy
from haystack.components.preprocessors import DocumentPreprocessor
p = DocumentPreprocessor(split_by='sentence', split_length=6)
strategy = HaystackChunkingStrategy(p)
```

Run tests:

```bash
pytest packages/orthant-haystack -q
```
