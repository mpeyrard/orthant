import dotenv
import os
import pytest


@pytest.fixture(scope="session")
def api_key() -> str:
    dotenv.load_dotenv(dotenv.find_dotenv(filename=".env", usecwd=True))
    key = os.environ.get("MISTRAL_API_KEY")
    if not key:
        pytest.skip(f"MISTRAL_API_KEY not set; skipping system tests. {os.getcwd()}")
    return key
