import numpy as np
import pytest
from orthant.mistral import MistralEmbeddingClient


def _cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    a = np.asarray(a, dtype=np.float32)
    b = np.asarray(b, dtype=np.float32)
    na = np.linalg.norm(a)
    nb = np.linalg.norm(b)
    if na == 0.0 or nb == 0.0:
        return 0.0  # or np.nan if you prefer
    sim = float(np.dot(a, b) / (na * nb))
    # optional: clamp for tiny numerical drift
    return float(np.clip(sim, -1.0, 1.0))



@pytest.mark.system
class TestMistralEmbeddingClient:
    @pytest.mark.asyncio
    async def test_document_embedding(self, api_key):
        print(api_key)
        subject = MistralEmbeddingClient(api_key=api_key)
        embedding_vector = await subject.encode_document_async(
            document="We build on prior synthetic evaluations, most notably RULER (Hsieh et al., 2024), to benchmark of long-context LLM capabilities. Most of these are largely based on the “needle-in-a-haystack” framework (Kamradt, 2023), which has gained popularity due to its ease of evaluation and modification (Yuan et al., 2024; Xu et al., 2024; Song et al., 2025; Laban et al., 2024; Sharma et al., 2024). Outside of NIAH, the recent LongReason benchmark (Ling et al., 2025) expands the context of short-context reasoning questions to evaluate long-context capabilities, while GSM-∞(Zhou et al., 2025) generates long-context tasks with controllable complexity and information density via computational graphs."
        )
        assert len(embedding_vector) == 1024

    @pytest.mark.asyncio
    async def test_document_embedding_batch(self, api_key):
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
    async def test_document_embeddings_are_consistent(self, api_key):
        subject = MistralEmbeddingClient(api_key=api_key)
        embedding1 = await subject.encode_document_async(document="The ball is red.")
        embedding2 = await subject.encode_document_async(document="The ball is red.")
        sim = _cosine_similarity(np.array(embedding1), np.array(embedding2))
        distance = 1.0 - sim
        assert distance <= 0.005, f"Cosine distance too large: {distance} (similarity={sim})"
