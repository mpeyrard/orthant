from typing import Protocol, runtime_checkable

from .dataset_spec import DatasetSpec


@runtime_checkable
class DatasetRegistry(Protocol):
    """Dataset registry"""

    def insert(self, dataset_spec: DatasetSpec) -> None: ...
    def get_dataset(self, dataset_id: str) -> DatasetSpec | None: ...
    def list_datasets(self) -> list[DatasetSpec]: ...


class InMemoryDatasetRegistry:
    """In-memory dataset registry"""

    def __init__(self):
        self._datasets: dict[str, DatasetSpec] = {}

    def insert(self, dataset_spec: DatasetSpec) -> None:
        if dataset_spec.dataset_id in self._datasets:
            raise ValueError(f"Dataset with id '{dataset_spec.dataset_id}' already exists.")
        self._datasets[dataset_spec.dataset_id] = dataset_spec

    def get_dataset(self, dataset_id: str) -> DatasetSpec | None:
        return self._datasets.get(dataset_id, None)

    def list_datasets(self) -> list[DatasetSpec]:
        return list(self._datasets.values())
