"""Mistral embedding client implementation.

This module provides `MistralEmbeddingClient`, an implementation of the
`EmbeddingClient` protocol that delegates to the official `mistralai` SDK.
It supports single and batched document/query embedding methods and accepts
an API key and model name at construction.
"""

from mistralai import Mistral
from typing import Sequence
from orthant.embedding.embedding_client import EmbeddingClient


class MistralEmbeddingClient(EmbeddingClient):
    """Mistral embedding client backed by the `mistralai` SDK.

    Usage:
        client = MistralEmbeddingClient(api_key="...")
        emb = await client.encode_document_async("hello world")

    Args:
        api_key: Mistral API key used to construct the SDK client.
        model: Model name used for the embeddings endpoint.
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
        """Encode a single document string into an embedding vector.

        This delegates to `encode_document_batch_async` for implementation
        simplicity.
        """
        embeddings = await self.encode_document_batch_async([document])
        return embeddings[0]

    async def encode_document_batch_async(self, documents: Sequence[str]) -> Sequence[Sequence[float]]:
        """Encode a batch of documents.

        Calls the SDK's async embeddings endpoint and returns a list of
        embedding vectors in the same order as `documents`.
        """
        if not documents:
            return []
        embeddings_result = await self._client.embeddings.create_async(
            model=self._model,
            inputs=documents,
        )
        return [e.embedding for e in embeddings_result.data]

    async def encode_query_async(self, query: str) -> Sequence[float]:
        """Encode a single query string; delegated to document encoder."""
        return await self.encode_document_async(query)

    async def encode_query_batch_async(self, queries: Sequence[str]) -> Sequence[Sequence[float]]:
        """Encode a batch of queries; delegated to document batch encoder."""
        return await self.encode_document_batch_async(queries)

    def get_embedding_dim(self) -> int:
        return 1024
