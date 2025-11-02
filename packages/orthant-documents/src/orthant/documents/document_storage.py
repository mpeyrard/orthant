from typing import runtime_checkable, Protocol
from .document_contracts import OrthantDocument


@runtime_checkable
class OrthantDocumentStorage(Protocol):
    """Document storage protocol for persisting documents to storage backends like LanceDB or ElasticSearch"""

    async def store_async(self, document: OrthantDocument):
        ...

    async def store_batch_async(self, documents: list[OrthantDocument]):
        ...
