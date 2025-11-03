"""Dependency injection container for the datasets package.

This module defines a simple DeclarativeContainer providing the dataset
registry and dataset service singletons used by the datasets package.
"""

from dependency_injector import containers, providers

from .dataset_registry import InMemoryDatasetRegistry
from .dataset_service import DatasetService


class DatasetsContainer(containers.DeclarativeContainer):
    """DI container for dataset-related services.

    Provides singletons for `InMemoryDatasetRegistry` and `DatasetService`.
    Applications can override or extend this container to supply alternate
    implementations (e.g., a persistent registry).
    """

    dataset_registry = providers.Singleton(InMemoryDatasetRegistry)
    dataset_service = providers.Singleton(DatasetService, dataset_registry=dataset_registry)
