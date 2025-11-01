from pydantic import BaseModel, Field
from typing import runtime_checkable, Protocol


class OrthantDocumentNode(BaseModel):
    """Represents a text node in a document."""
    node_path: str
    content: str

class OrthantDocument(BaseModel):
    """Represents a document."""
    document_id: str
    source_uri: str
    nodes: list[OrthantDocumentNode] = Field(default_factory=list)

class OrthantDocumentNodeChunk(BaseModel):
    """Represents a chunk of a document node. Chunks are derived from nodes."""
    document_id: str
    node_path: str
    node_chunk_index: int
    content: str

@runtime_checkable
class DocumentReader(Protocol):
    """Document reader protocol"""
    def read_file(self, file_uri: str) -> OrthantDocument:
        ...
