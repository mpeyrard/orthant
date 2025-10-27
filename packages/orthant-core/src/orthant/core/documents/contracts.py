from pydantic import BaseModel
from typing import runtime_checkable, IO, Protocol


class TextDocumentNode(BaseModel):
    """Represents a text node in a document."""
    node_path: str
    content: str

class ImageDocumentNode(BaseModel):
    """Represents an image node in a document."""
    node_path: str
    content: bytes

class Document(BaseModel):
    """Represents a document."""
    document_id: str
    source_uri: str
    text_nodes: list[TextDocumentNode] = []
    image_nodes: list[ImageDocumentNode] = []

class DocumentNodeChunk(BaseModel):
    """Represents a chunk of a document node. Chunks are derived from nodes."""
    document_id: str
    node_path: str
    node_chunk_index: int
    content: str

@runtime_checkable
class DocumentReader(Protocol):
    """Document reader protocol"""
    def read_file(self, file: IO[str] | IO[bytes]) -> Document:
        ...
