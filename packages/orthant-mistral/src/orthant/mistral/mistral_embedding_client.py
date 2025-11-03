from mistralai import Mistral
from typing import Sequence
from orthant.embedding.embedding_client import EmbeddingClient


class MistralEmbeddingClient(EmbeddingClient):
    """
    Mistral embedding client that uses the official `mistralai` SDK.
    This implementation delegates to a `mistralai.Mistral` instance for embeddings.
    By default, it will create its own client from `api_key`, but callers can inject a preconfigured `Mistral` instance (useful for testing or sharing a single HTTP client).
    """

    def __init__(
        self,
        api_key: str,
        model: str = "mistral-embed",
    ) -> None:
        self._api_key = api_key
        self._model = model
        self._client = Mistral(api_key=self._api_key)

    async def encode_document_async(self, document: str) -> Sequence[float]:
        embeddings = await self.encode_document_batch_async([document])
        return embeddings[0]

    async def encode_document_batch_async(self, documents: Sequence[str]) -> Sequence[Sequence[float]]:
        if not documents:
            return []
        embeddings_result = await self._client.embeddings.create_async(
            model=self._model,
            inputs=documents,
        )
        return [e.embedding for e in embeddings_result.data]

    async def encode_query_async(self, query: str) -> Sequence[float]:
        return await self.encode_document_async(query)

    async def encode_query_batch_async(self, queries: Sequence[str]) -> Sequence[Sequence[float]]:
        return await self.encode_document_batch_async(queries)
