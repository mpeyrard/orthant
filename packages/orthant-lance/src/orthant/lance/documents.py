"""LanceDB document helpers and schema factory.

This module contains helpers to create a PyArrow schema suitable for LanceDB
and a helper to build Lance-compatible document records.
"""
import datetime as dt
import pyarrow as pa
from typing import Any


def make_document_chunk_lance_schema(n_dims: int) -> pa.Schema:
    """Create a PyArrow schema for storing document chunks in Lance.

    Args:
        n_dims: The embedding vector dimension. This creates a fixed-size list
            field for `embedding` with the specified number of float32 values.

    Returns:
        A `pyarrow.Schema` instance describing the document chunk record.
    """
    lance_schema = pa.schema([
        pa.field("source_uri", pa.string()),
        pa.field("node_path", pa.string()),
        pa.field("node_chunk_index", pa.int64()),
        pa.field("modality", pa.string()),
        pa.field("created_at", pa.timestamp("us")),
        pa.field("content", pa.string()),
        pa.field("embedding", pa.list_(pa.float32(), n_dims)),
    ])
    return lance_schema


def make_document_chunk(
        source_uri: str,
        node_path: str,
        node_chunk_index: int,
        modality: str,
        created_at: dt.datetime,
        content: str,
        embedding: list[float]
) -> dict[str, Any]:
    """Build a Lance-compatible record dictionary for a document chunk.

    Args:
        source_uri: Original document source URI
        node_path: Path for the node within the document
        node_chunk_index: Index of the chunk within the node
        modality: Modality label (e.g., 'text', 'image')
        created_at: Creation timestamp (timezone-aware)
        content: The textual content of the chunk
        embedding: Embedding vector for the chunk

    Returns:
        Dictionary matching the field names defined in the Lance schema.
    """
    return dict(
        source_uri=source_uri,
        node_path=node_path,
        node_chunk_index=node_chunk_index,
        modality=modality,
        created_at=created_at,
        content=content,
        embedding=embedding,
    )
