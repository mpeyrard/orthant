"""Document storage protocol.

This module defines the `OrthantDocumentStorage` protocol which storage
backends should implement to persist documents and document batches.
"""
from typing import runtime_checkable, Protocol
from .document_contracts import OrthantDocument


@runtime_checkable
class OrthantDocumentStorage(Protocol):
    """Document storage protocol for persisting documents to storage backends.

    Implementations should persist documents provided via the async methods
    and may perform batching/optimizations as appropriate. Methods are
    intentionally simple to allow a variety of backends (databases, vector
    stores, object stores) to implement this contract.
    """

    async def store_async(self, document: OrthantDocument):
        ...

    async def store_batch_async(self, documents: list[OrthantDocument]):
        ...
