from dataclasses import dataclass


@dataclass
class DatasetSpec:
    dataset_id: str
    storage_format: str
    query_format: str
    uri: str | None = None
    glob: str | None = None
