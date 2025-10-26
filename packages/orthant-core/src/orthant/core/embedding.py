from typing import Protocol, runtime_checkable


@runtime_checkable
class EmbeddingModel(Protocol):
    """Embedding model"""

    def encode(self, text: str) -> list[float]:
        ...

    async def encode_async(self, text: str) -> list[float]:
        ...

    def encode_batch(self, batch: list[str]) -> list[list[float]]:
        ...

    async def encode_batch_async(self, batch: list[str]) -> list[list[float]]:
        ...
