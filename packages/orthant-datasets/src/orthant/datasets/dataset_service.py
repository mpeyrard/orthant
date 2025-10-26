from .dataset_registry import DatasetRegistry
from .dataset_spec import DatasetSpec


class DatasetService:
    """Dataset CRUD operations"""

    def __init__(self, dataset_registry: DatasetRegistry):
        self._dataset_registry = dataset_registry

    def add_dataset(self, dataset_spec: DatasetSpec):
        """Adds a new dataset to the registry."""
        self._dataset_registry.insert(dataset_spec)

    def get_dataset(self, dataset_id: str) -> DatasetSpec | None:
        """Gets a dataset from the registry."""
        return self._dataset_registry.get_dataset(dataset_id)

    def list_datasets(self) -> list[DatasetSpec]:
        """Lists all datasets in the registry."""
        return self._dataset_registry.list_datasets()
