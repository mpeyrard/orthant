"""
Dataset specification data model.
Defines the `DatasetSpec` dataclass which contains the minimal metadata
required to register and locate a dataset (storage format, URI, etc.).
"""

from dataclasses import dataclass


@dataclass
class DatasetSpec:
    """Specification for a dataset.

    Attributes:
        dataset_id: Unique identifier for the dataset.
        storage_format: Storage format identifier (e.g., 'lance', 's3', etc.).
        query_format: Query format hint (e.g., 'sql', 'lance-query').
        uri: Optional URI where the dataset is stored.
        glob: Optional glob pattern to match files.
    """

    dataset_id: str
    storage_format: str
    query_format: str
    uri: str | None = None
    glob: str | None = None
