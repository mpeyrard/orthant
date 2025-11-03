orthant-runtime
================

Runtime helpers for Orthant: logging, configuration, and storage initialization.

Install (editable, dev):

```bash
. .venv/bin/activate
python -m pip install -e packages/orthant-runtime
```

Quick usage:

```python
from orthant.runtime import load_orthant_config, initialize_logging
initialize_logging()
cfg = load_orthant_config()
```

Run tests: (no tests currently)

```bash
pytest packages/orthant-runtime -q
```
