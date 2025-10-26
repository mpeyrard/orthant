from typing import runtime_checkable, Protocol


@runtime_checkable
class ChunkingStrategy(Protocol):
    """Text chunking strategy"""
    def split_text(self, text: str) -> list[str]: ...
