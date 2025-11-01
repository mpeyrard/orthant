from typing import runtime_checkable, Protocol

from ..documents import OrthantDocument, OrthantDocumentNodeChunk


@runtime_checkable
class ChunkingStrategy(Protocol):
    """Text chunking strategy"""
    def chunk_document(self, document: OrthantDocument) -> list[OrthantDocumentNodeChunk]: ...
