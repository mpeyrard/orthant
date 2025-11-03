"""High-level dataset service for CRUD operations.

This module provides a thin service layer around `DatasetRegistry` to
encapsulate dataset management operations used by higher-level application
code or REST APIs.
"""

from .dataset_registry import DatasetRegistry
from .dataset_spec import DatasetSpec


class DatasetService:
    """Dataset CRUD operations service.

    The service delegates storage concerns to a `DatasetRegistry` instance,
    allowing different registry implementations to be wired in (e.g., in-memory
    or persistent).
    """

    def __init__(self, dataset_registry: DatasetRegistry):
        self._dataset_registry = dataset_registry

    def add_dataset(self, dataset_spec: DatasetSpec):
        """Add a new dataset to the registry.

        Raises ValueError if the dataset already exists (delegated from registry).
        """
        self._dataset_registry.insert(dataset_spec)

    def get_dataset(self, dataset_id: str) -> DatasetSpec | None:
        """Retrieve a DatasetSpec by its id."""
        return self._dataset_registry.get_dataset(dataset_id)

    def list_datasets(self) -> list[DatasetSpec]:
        """Return all registered DatasetSpec entries."""
        return self._dataset_registry.list_datasets()
