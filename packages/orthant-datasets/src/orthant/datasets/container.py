from dependency_injector import containers, providers

from .dataset_registry import InMemoryDatasetRegistry
from .dataset_service import DatasetService


class DatasetsContainer(containers.DeclarativeContainer):
    """DI container for the dataset package"""
    dataset_registry = providers.Singleton(InMemoryDatasetRegistry)
    dataset_service = providers.Singleton(DatasetService, dataset_registry=dataset_registry)
