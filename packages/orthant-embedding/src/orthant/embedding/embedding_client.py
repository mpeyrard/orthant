from typing import runtime_checkable, Protocol, Sequence


"""Embedding client protocol

This module defines the `EmbeddingClient` protocol which acts as an interface
for embedding providers used by Orthant. It's runtime-checkable so concrete
implementations can be verified with `isinstance(obj, EmbeddingClient)` when
necessary.

Notes:
- Methods are asynchronous and return sequences of floats (embedding vectors).
- Batch methods accept an iterable/sequence of strings and return a sequence of
  embedding vectors in the same order.
"""


@runtime_checkable
class EmbeddingClient(Protocol):
    """Protocol (interface) for embedding providers.

    Implementations should provide asynchronous methods to encode documents and
    queries into embedding vectors. The protocol intentionally keeps the exact
    vector numeric type and precision unspecified — use `float` values and a
    consistent dimensionality across calls.
    """

    async def encode_document_async(self, document: str) -> Sequence[float]:
        """Encode a single document (text) into an embedding vector.

        Args:
            document: The document text to encode.

        Returns:
            A sequence of floats representing the embedding.
        """  # pragma: no cover
        ...

    async def encode_document_batch_async(self, documents: Sequence[str]) -> Sequence[Sequence[float]]:
        """Encode a batch of documents.

        Args:
            documents: Sequence of document texts to encode.

        Returns:
            A sequence (same order) of embedding vectors.
        """  # pragma: no cover
        ...

    async def encode_query_async(self, query: str) -> Sequence[float]:
        """Encode a query string into an embedding vector.

        Args:
            query: The query text to encode.

        Returns:
            A sequence of floats representing the query embedding.
        """  # pragma: no cover
        ...

    async def encode_query_batch_async(self, queries: Sequence[str]) -> Sequence[Sequence[float]]:
        """Encode a batch of query strings.

        Args:
            queries: Sequence of query strings to encode.

        Returns:
            A sequence (same order) of query embedding vectors.
        """  # pragma: no cover
        ...
