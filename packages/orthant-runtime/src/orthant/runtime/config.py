from pydantic import BaseModel, Field


class OrthantConfig(BaseModel):
    storage_dir: str = Field(..., alias="storage", description="Storage directory for datasets.")
