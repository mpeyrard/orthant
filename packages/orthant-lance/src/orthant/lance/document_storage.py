import datetime as dt
import lancedb
from typing import Optional, Any

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
        table_name: str = "documents",
        embedding_dim: Optional[int] = None,
    ):
        """
        Initialize the Lance document storage.

        Args:
            uri: Path to the LanceDB database
            table_name: Name of the table to store documents (default: "documents")
            embedding_dim: Dimension of the embedding vectors. If None, schema won't include embeddings.
        """
        self.uri = uri
        self.table_name = table_name
        self.embedding_dim = embedding_dim
        self._db = lancedb.connect(uri)
        self._schema = make_document_chunk_lance_schema(embedding_dim) if embedding_dim else None
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
                "node_chunk_index": 0,  # Full node as single chunk
                "modality": "text",  # Default modality
                "created_at": created_at,
                "content": node.content,
            }

            # Only add embedding if schema supports it
            if self.embedding_dim:
                # Initialize with zero vector if no embedding provided
                record["embedding"] = [0.0] * self.embedding_dim

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
            "source_uri": chunk.document_id,  # Using document_id as source_uri
            "node_path": chunk.node_path,
            "node_chunk_index": chunk.node_chunk_index,
            "modality": "text",  # Default modality
            "created_at": dt.datetime.now(dt.timezone.utc),
            "content": chunk.content,
        }

        # Only add embedding if schema supports it
        if self.embedding_dim:
            # Initialize with zero vector if no embedding provided
            record["embedding"] = [0.0] * self.embedding_dim

        return record

    async def store_async(self, document: OrthantDocument) -> None:
        """
        Store a single document asynchronously.

        Args:
            document: The document to store
        """
        records = self._document_to_lance_records(document)

        if not records:
            return

        # Add records to table, creating it if needed
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
