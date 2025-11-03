"""Content loading abstractions.

Provides the `ContentLoader` protocol for loading document content from URIs
and a default implementation that uses `fsspec`.
"""

import fsspec
from typing import runtime_checkable, Protocol


@runtime_checkable
class ContentLoader(Protocol):
    """Protocol that defines methods to load document content.

    Implementations must provide synchronous and asynchronous variants for
    loading text and bytes from a given URI. URIs may be local paths, data
    URLs, or fsspec-supported remote file systems (memory://, zip://, s3://, etc.).
    """
    def load_text(self, uri: str, *, encoding: str = "utf-8") -> str: ...
    def load_bytes(self, uri: str) -> bytes: ...
    async def load_text_async(self, uri: str, *, encoding: str = "utf-8") -> str: ...
    async def load_bytes_async(self, uri: str) -> bytes: ...


class DefaultContentLoader(ContentLoader):
    """Default ContentLoader using fsspec.

    This loader delegates to `fsspec.open` so it supports many protocols and
    transparently handles local and remote files. The async methods delegate
    to the synchronous methods; callers that require true async I/O can
    replace this with a specialized implementation.
    """
    def load_text(self, uri: str, *, encoding: str = "utf-8") -> str:
        """Load text from a URI using fsspec and return it as str."""
        with fsspec.open(uri, "r", encoding=encoding) as f:
            return f.read()

    def load_bytes(self, uri: str) -> bytes:
        """Load bytes from a URI using fsspec and return raw bytes."""
        with fsspec.open(uri, "rb") as f:
            return f.read()

    async def load_text_async(self, uri: str, *, encoding: str = "utf-8") -> str:
        """Async wrapper around the synchronous text loader.

        Note: This implementation is not truly asynchronous. It is provided for
        API compatibility; replace with a proper async implementation if
        non-blocking I/O is required.
        """
        return self.load_text(uri, encoding=encoding)

    async def load_bytes_async(self, uri: str) -> bytes:
        """Async wrapper around the synchronous bytes loader."""
        return self.load_bytes(uri)
