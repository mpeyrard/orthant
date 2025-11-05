from typing import runtime_checkable, Protocol
from .document_contracts import VectorDocumentNodeChunk


@runtime_checkable
class VectorDocumentStorage(Protocol):
    """Protocol for storing and retrieving vector documents."""
    async def store_vector_document_async(self, document: VectorDocumentNodeChunk) -> None: ...
