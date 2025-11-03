import datetime as dt
import pyarrow as pa
from typing import Any


def make_document_chunk_lance_schema(n_dims: int) -> pa.Schema:
    """Creates the data type to represent a document chunk in Lance."""
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
    return dict(
        source_uri=source_uri,
        node_path=node_path,
        node_chunk_index=node_chunk_index,
        modality=modality,
        created_at=created_at,
        content=content,
        embedding=embedding,
    )
