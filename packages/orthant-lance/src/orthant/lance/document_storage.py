"""LanceDB-backed document storage implementation.

This module provides `LanceDocumentStorage`, a concrete storage implementation
that writes document nodes and chunks into a LanceDB table. The storage
expects a fixed embedding dimensionality (embedding_dim) and will include an
`embedding` field for each record.
"""
import datetime as dt
import lancedb
from typing import Any

from orthant.documents import OrthantDocument, OrthantDocumentNodeChunk
from .documents import make_document_chunk_lance_schema


class LanceDocumentStorage:
    """
    Document storage implementation using LanceDB.

    This class implements the OrthantDocumentStorage protocol for storing
    documents and document chunks in LanceDB.
    """

    def __init__(
        self,
        uri: str,
        table_name: str,
        embedding_dim: int,
    ):
        """
        Initialize the Lance document storage.

        Args:
            uri: Path to the LanceDB database
            table_name: Name of the table to store documents (required)
            embedding_dim: Dimension of the embedding vectors (required, must be > 0)
        """
        self.uri = uri
        self.table_name = table_name
        self.embedding_dim = embedding_dim
        self._db = lancedb.connect(uri)
        # Always create a schema for document chunks since embedding_dim is required
        self._schema = make_document_chunk_lance_schema(embedding_dim)
        self._table = None

    def _get_or_create_table(self):
        """Get an existing table or create a new one."""
        if self._table is None:
            try:
                self._table = self._db.open_table(self.table_name)
            except Exception:
                # Table doesn't exist, will be created on first add
                pass
        return self._table

    def _document_to_lance_records(self, document: OrthantDocument) -> list[dict[str, Any]]:
        """
        Convert an OrthantDocument to Lance record format.

        Args:
            document: The document to convert

        Returns:
            List of Lance-compatible records (one per node)
        """
        records = []
        created_at = dt.datetime.now(dt.timezone.utc)
        for node in document.nodes:
            record: dict[str, Any] = {
                "source_uri": document.source_uri,
                "node_path": node.node_path,
                "node_chunk_index": 0,
                "modality": "text",
                "created_at": created_at,
                "content": node.content,
                "embedding": [0.0] * self.embedding_dim
            }
            records.append(record)
        return records

    def _chunk_to_lance_record(self, chunk: OrthantDocumentNodeChunk) -> dict[str, Any]:
        """
        Convert an OrthantDocumentNodeChunk to Lance record format.

        Args:
            chunk: The chunk to convert

        Returns:
            Lance-compatible record
        """
        record: dict[str, Any] = {
            "source_uri": chunk.document_id,
            "node_path": chunk.node_path,
            "node_chunk_index": chunk.node_chunk_index,
            "modality": "text",
            "created_at": dt.datetime.now(dt.timezone.utc),
            "content": chunk.content,
            "embedding": [0.0] * self.embedding_dim
        }
        return record

    async def store_async(self, document: OrthantDocument) -> None:
        """
        Store a single document asynchronously.

        This method converts the document into LanceDB record format and adds
        the records to the specified table. If the table does not exist, it
        will be created. The embedding field in the records is initialized
        to zero vectors.

        Args:
            document: The document to store
        """
        records = self._document_to_lance_records(document)

        if not records:
            return

        # Add records to the table, creating it if needed
        if self._table is None:
            self._table = self._db.create_table(
                self.table_name,
                data=records,
                mode="overwrite"
            )
        else:
            self._table.add(records)

    async def store_batch_async(self, documents: list[OrthantDocument]) -> None:
        """
        Store multiple documents asynchronously.

        This method converts each document in the list into LanceDB record
        format and adds all the records to the specified table. The table
        will be created if it does not exist. Each record's embedding field
        is initialized to a zero vector.

        Args:
            documents: List of documents to store
        """
        if not documents:
            return

        # Convert all documents to records
        all_records = []
        for document in documents:
            records = self._document_to_lance_records(document)
            all_records.extend(records)

        if not all_records:
            return

        # Add all records to table, creating it if needed
        if self._table is None:
            self._table = self._db.create_table(
                self.table_name,
                data=all_records,
                mode="overwrite"
            )
        else:
            self._table.add(all_records)

    async def store_chunks_async(self, chunks: list[OrthantDocumentNodeChunk]) -> None:
        """
        Store document chunks asynchronously.

        This method converts each chunk in the list into LanceDB record format
        and adds all the records to the specified table. The table will be
        created if it does not exist. Each record's embedding field is
        initialized to a zero vector.

        Args:
            chunks: List of chunks to store
        """
        if not chunks:
            return

        # Convert chunks to Lance record format
        records = [self._chunk_to_lance_record(chunk) for chunk in chunks]

        # Add records to table, creating it if needed
        if self._table is None:
            self._table = self._db.create_table(
                self.table_name,
                data=records,
                mode="overwrite"
            )
        else:
            self._table.add(records)
