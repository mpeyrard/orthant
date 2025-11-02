from typing import Protocol, runtime_checkable


@runtime_checkable
class VectorStore(Protocol):
    """Protocol for vector storage backends."""

    def store_chunks(self, chunks: list) -> None:
        """Store embedded document chunks synchronously."""
        ...

    async def store_chunks_async(self, chunks: list) -> None:
        """Store embedded document chunks asynchronously."""
        ...
