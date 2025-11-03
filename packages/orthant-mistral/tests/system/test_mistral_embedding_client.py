import sys
from pathlib import Path
import os

# Add workspace package src directories to sys.path so tests can import `orthant.*`
repo_root = Path(__file__).resolve()
for parent in repo_root.parents:
    if (parent / "pyproject.toml").exists() or (parent / ".git").exists():
        repo_root = parent
        break
packages_dir = repo_root / "packages"
if packages_dir.exists():
    for child in packages_dir.iterdir():
        src = child / "src"
        if src.exists():
            sys.path.insert(0, str(src))

import dotenv
import pytest
from pathlib import Path

# Import the client directly from the module to avoid relying on package __init__
from orthant.mistral.mistral_embedding_client import MistralEmbeddingClient


def _has_env(p: Path):
    return os.path.isfile(p / ".env")


def _load_env():
    current_dir = Path(os.getcwd())
    dir_to_check = current_dir
    while True:
        if _has_env(dir_to_check):
            dotenv.load_dotenv(dotenv_path=dir_to_check / ".env")
            return
        if dir_to_check.parent == dir_to_check:
            # Reached root directory
            break
        dir_to_check = dir_to_check.parent

_load_env()
api_key = os.environ.get("MISTRAL_API_KEY")

# If no API key, skip all tests in this module (useful for local runs)
if not api_key:
    pytest.skip("MISTRAL_API_KEY not set; skipping system tests", allow_module_level=True)

@pytest.mark.system
class TestMistralEmbeddingClient:
    @pytest.mark.asyncio
    async def test_document_embedding(self):
        subject = MistralEmbeddingClient(api_key=api_key)
        embedding_vector = await subject.encode_document_async(
            document="We build on prior synthetic evaluations, most notably RULER (Hsieh et al., 2024), to benchmark of long-context LLM capabilities. Most of these are largely based on the “needle-in-a-haystack” framework (Kamradt, 2023), which has gained popularity due to its ease of evaluation and modification (Yuan et al., 2024; Xu et al., 2024; Song et al., 2025; Laban et al., 2024; Sharma et al., 2024). Outside of NIAH, the recent LongReason benchmark (Ling et al., 2025) expands the context of short-context reasoning questions to evaluate long-context capabilities, while GSM-∞(Zhou et al., 2025) generates long-context tasks with controllable complexity and information density via computational graphs."
        )
        assert len(embedding_vector) == 1024

    @pytest.mark.asyncio
    async def test_document_embedding_batch(self):
        subject = MistralEmbeddingClient(api_key=api_key)
        embedding_vectors = await subject.encode_document_batch_async(
            documents=[
                "The ball is red.",
                "The ball is blue.",
                "The ball is green.",
            ]
        )
        assert len(embedding_vectors) == 3
        assert len(embedding_vectors[0]) == 1024
        assert len(embedding_vectors[1]) == 1024
        assert len(embedding_vectors[2]) == 1024

    @pytest.mark.asyncio
    async def test_document_embeddings_are_consistent(self):
        subject = MistralEmbeddingClient(api_key=api_key)
        embedding1 = await subject.encode_document_async(document="The ball is red.")
        embedding2 = await subject.encode_document_async(document="The ball is red.")

        # Use cosine similarity to test consistency between repeated calls.
        # Cosine similarity near 1.0 indicates the vectors are aligned; we allow
        # small numerical differences with a threshold.
        import math

        def cosine(a, b):
            dot = 0.0
            na = 0.0
            nb = 0.0
            for x, y in zip(a, b):
                dot += float(x) * float(y)
                na += float(x) * float(x)
                nb += float(y) * float(y)
            if na == 0 or nb == 0:
                return 0.0
            return dot / (math.sqrt(na) * math.sqrt(nb))

        assert len(embedding1) == len(embedding2)
        sim = cosine(embedding1, embedding2)
        # threshold can be tuned; using cosine distance (1 - sim) for clarity
        distance = 1.0 - sim
        assert distance <= 0.005, f"Cosine distance too large: {distance} (similarity={sim})"
