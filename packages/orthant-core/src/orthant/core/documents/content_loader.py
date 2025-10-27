import fsspec
from typing import runtime_checkable, Protocol


@runtime_checkable
class ContentLoader(Protocol):
    def load_text(self, uri: str, *, encoding: str = "utf-8") -> str: ...
    def load_bytes(self, uri: str) -> bytes: ...
    async def load_text_async(self, uri: str, *, encoding: str = "utf-8") -> str: ...
    async def load_bytes_async(self, uri: str) -> bytes: ...

class DefaultContentLoader:
    def load_text(self, uri: str, *, encoding: str = "utf-8") -> str:
        """Load text from a URI"""
        with fsspec.open(uri, "r", encoding=encoding) as f:
            return f.read()

    def load_bytes(self, uri: str) -> bytes:
        """Load bytes from a URI"""
        with fsspec.open(uri, "rb") as f:
            return f.read()

    async def load_text_async(self, uri: str, *, encoding: str = "utf-8") -> str:
        """Load text from a URI asynchronously"""
        return self.load_text(uri, encoding=encoding)

    async def load_bytes_async(self, uri: str) -> bytes:
        """Load bytes from a URI asynchronously"""
        return self.load_bytes(uri)
