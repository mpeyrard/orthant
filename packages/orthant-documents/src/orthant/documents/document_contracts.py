"""Pydantic models for Orthant documents and document pieces.

This module defines the canonical data contracts used throughout Orthant to
represent documents, nodes, and node chunks. These are Pydantic models so they
can be validated and serialized easily.
"""

from pydantic import BaseModel, Field


class OrthantDocumentNode(BaseModel):
    """Represents a text node (a logical unit) within a document.

    Attributes:
        node_path: Unique path within the document that identifies the node.
        content: Textual content of the node.
    """
    node_path: str
    content: str

class OrthantDocument(BaseModel):
    """Top-level representation of a document.

    Attributes:
        document_id: Unique identifier for the document.
        source_uri: Original source URI (file://, http://, etc.).
        nodes: List of `OrthantDocumentNode` items contained in the document.
    """
    document_id: str
    source_uri: str
    nodes: list[OrthantDocumentNode] = Field(default_factory=list)

class OrthantDocumentNodeChunk(BaseModel):
    """Represents a chunked portion of a document node.

    Attributes:
        document_id: Identifier of the parent document.
        node_path: Path of the parent node within the document.
        node_chunk_index: Zero-based index of chunk within the node.
        content: Text content for the chunk.
    """
    document_id: str
    node_path: str
    node_chunk_index: int
    content: str
