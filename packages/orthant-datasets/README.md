orthant-datasets
=================

A small dataset registry and service used by Orthant.

Install (editable, dev):

```bash
. .venv/bin/activate
python -m pip install -e packages/orthant-datasets
```

Quick usage:

```python
from orthant.datasets import DatasetsContainer
container = DatasetsContainer()
service = container.dataset_service()
```

Run tests:

```bash
pytest packages/orthant-datasets -q
```

Notes:
- `DatasetSpec` is a simple dataclass describing dataset metadata.
- `InMemoryDatasetRegistry` is provided for tests/development.
