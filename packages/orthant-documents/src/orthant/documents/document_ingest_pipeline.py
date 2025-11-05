from typing import runtime_checkable, Protocol
from .document_contracts import OrthantDocument


@runtime_checkable
class DocumentIngestPipeline(Protocol):
    """Document ingest pipeline protocol."""
    async def ingest_batch_async(self, documents: list[OrthantDocument]) -> None: ...
