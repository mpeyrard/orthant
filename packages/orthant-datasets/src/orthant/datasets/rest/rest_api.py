import fastapi
import logging
from fastapi import Depends
from dependency_injector.wiring import inject, Provide

from ..container import DatasetsContainer
from ..dataset_service import DatasetService
from ..dataset_spec import DatasetSpec


api_router = fastapi.APIRouter()


@api_router.post("/v1/datasets", status_code=201)
@inject
async def add_dataset(
    dataset_spec: DatasetSpec = fastapi.Body(...),
    dataset_service: DatasetService = Depends(Provide[DatasetsContainer.dataset_service])
):
    dataset_service.add_dataset(dataset_spec)
    logging.info(f"Added dataset '{dataset_spec.dataset_id}'")


@api_router.get("/v1/datasets")
async def list_datasets():
    return None


@api_router.get("/v1/datasets/{dataset_id}")
async def get_dataset(dataset_id: str):
    return None
