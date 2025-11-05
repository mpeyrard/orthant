"""
LanceDB-backed document storage implementation.

This module provides a small, testable `LanceDocumentStorage` that accepts
both full `OrthantDocument` objects (document nodes) and already-chunked
`VectorDocumentNodeChunk` instances (which include an embedding `vector`).
"""
import datetime as dt
import lancedb
import pyarrow as pa
from typing import Any
from orthant.embedding import VectorDocumentStorage, VectorDocumentNodeChunk


def _make_document_chunk_lance_schema(n_dims: int) -> pa.Schema:
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


def _get_or_create_table(uri: str, table_name: str, schema: pa.Schema):
    """
    Open an existing Lance table at `uri` named `table_name`, or create
    an empty table if it does not exist.

    This is a minimal helper used by callers that only have a URI and
    table name; it returns the table-like object from lancedb.
    """
    db = lancedb.connect(uri)
    try:
        return db.open_table(table_name)
    except Exception:
        return db.create_table(table_name, data=[], mode="overwrite", schema=schema)


def _convert_to_lance_doc(vector_doc: VectorDocumentNodeChunk) -> dict[str, Any]:
    """Build a Lance-compatible record dictionary for a document chunk.

    Args:
        vector_doc: Vector document to convert into the Lance schema format

    Returns:
        Dictionary matching the field names defined in the Lance schema.
    """
    # Some VectorDocumentNodeChunk instances may not provide all optional
    # metadata fields (source_uri, modality, created_at). Use sensible
    # fallbacks so callers (and tests) don't fail when fields are omitted.
    source_uri = getattr(vector_doc, "source_uri", None) or getattr(vector_doc, "document_id", None)
    modality = getattr(vector_doc, "modality", "text")
    created_at = getattr(vector_doc, "created_at", None) or dt.datetime.now(dt.timezone.utc)
    return dict(
        source_uri=source_uri,
        node_path=vector_doc.node_path,
        node_chunk_index=vector_doc.node_chunk_index,
        modality=modality,
        created_at=created_at,
        content=vector_doc.content,
        embedding=vector_doc.vector,
    )


class LanceDocumentStorage(VectorDocumentStorage):
    """Minimal LanceDB-backed storage for document chunks and vectors.

    This class focuses on correctness and testability. It does not implement
    indexing or search — only persistent writes to a Lance table.
    """

    def __init__(self, uri: str, table_name: str, embedding_dim: int):
        self.uri = uri
        self.table_name = table_name
        self.embedding_dim = embedding_dim
        self._db = lancedb.connect(uri)
        self._schema = _make_document_chunk_lance_schema(embedding_dim)
        self._table = _get_or_create_table(uri, table_name, self._schema)

    async def store_vector_document_async(self, document: VectorDocumentNodeChunk) -> None:
        """Store a single pre-chunked vector document"""
        lance_doc = _convert_to_lance_doc(document)
        # lancedb expects an iterable/list of records
        self._table.add(data=[lance_doc])
