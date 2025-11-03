"""Chunking strategies for splitting documents into embedding-ready chunks.

Defines the `ChunkingStrategy` protocol used by embedding code to split a
`OrthantDocument` into a sequence of `OrthantDocumentNodeChunk` objects.
"""

from typing import Protocol, runtime_checkable
from orthant.documents import OrthantDocument, OrthantDocumentNodeChunk


@runtime_checkable
class ChunkingStrategy(Protocol):
    """Protocol for text chunking strategies.

    Implementations should split an `OrthantDocument` into smaller
    `OrthantDocumentNodeChunk` pieces suitable for embedding and indexing.
    """

    def chunk_document(self, document: OrthantDocument) -> list[OrthantDocumentNodeChunk]: ...
