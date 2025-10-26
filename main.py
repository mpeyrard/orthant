import dotenv
import fastapi

dotenv.load_dotenv()
from orthant.datasets import DatasetsContainer
from orthant.datasets.rest import datasets_router
from orthant.runtime import initialize_logging, initialize_storage, load_orthant_config


initialize_logging()
orthant_config = load_orthant_config()
initialize_storage(orthant_config)


app = fastapi.FastAPI()
app.include_router(datasets_router, prefix="/api")

dataset_container = DatasetsContainer()

import orthant.datasets as orthant_datasets
import orthant.datasets.rest.rest_api as orthant_datasets_rest
dataset_container.wire(packages=
    [
        orthant_datasets,
        orthant_datasets_rest
    ]
)


def main():
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
