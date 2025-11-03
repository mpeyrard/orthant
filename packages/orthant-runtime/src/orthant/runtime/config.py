"""Runtime configuration models for Orthant.

Defines the `OrthantConfig` Pydantic model used to represent runtime
configuration such as the storage directory.
"""

from pydantic import BaseModel, Field


class OrthantConfig(BaseModel):
    """Orthant runtime configuration model.

    Fields:
        storage_dir: Directory path where datasets and storage files are kept.
    """
    storage_dir: str = Field(..., alias="storage", description="Storage directory for datasets.")
