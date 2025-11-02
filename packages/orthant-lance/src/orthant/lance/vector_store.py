import lancedb
import datetime as dt

from orthant.core.ingestion import EmbeddedDocumentChunk
from .documents import make_document_chunk_lance_schema




class LanceVectorStore:
    """
    Vector store implementation using LanceDB.

    This class provides storage and retrieval of embedded document chunks
    using LanceDB as the backend.
    """

    def __init__(self, uri: str, table_name: str = "documents", embedding_dim: int = 384):
        """
        Initialize the Lance vector store.

        Args:
            uri: Path to the LanceDB database
            table_name: Name of the table to store documents (default: "documents")
            embedding_dim: Dimension of the embedding vectors (default: 384)
        """
        self.uri = uri
        self.table_name = table_name
        self.embedding_dim = embedding_dim
        self._db = lancedb.connect(uri)
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

    def store_chunks(self, chunks: list["EmbeddedDocumentChunk"]) -> None:
        """
        Store embedded document chunks in the vector store.

        Args:
            chunks: List of embedded chunks to store
        """
        if not chunks:
            return

        # Convert chunks to Lance schema format
        lance_records = []
        for chunk in chunks:
            record = {
                "source_uri": chunk.source_uri,
                "node_path": chunk.node_path,
                "node_chunk_index": chunk.node_chunk_index,
                "modality": chunk.modality,
                "created_at": chunk.created_at or dt.datetime.now(dt.timezone.utc),
                "content": chunk.content,
                "embedding": chunk.embedding,
            }
            lance_records.append(record)

        # Add records to table, creating it if needed
        if self._table is None:
            self._table = self._db.create_table(
                self.table_name,
                data=lance_records,
                mode="overwrite"
            )
        else:
            self._table.add(lance_records)

    async def store_chunks_async(self, chunks: list["EmbeddedDocumentChunk"]) -> None:
        """
        Store embedded document chunks asynchronously.

        Args:
            chunks: List of embedded chunks to store
        """
        # LanceDB doesn't have native async support yet, so we use the sync method
        self.store_chunks(chunks)

    def search(self, query_vector: list[float], limit: int = 10) -> list["EmbeddedDocumentChunk"]:
        """
        Search for similar chunks using a query vector.

        Args:
            query_vector: Query embedding vector
            limit: Maximum number of results to return

        Returns:
            List of similar embedded chunks
        """
        from orthant.core.ingestion import EmbeddedDocumentChunk

        table = self._get_or_create_table()

        # Perform vector search
        results = table.search(query_vector).limit(limit).to_list()

        # Convert results back to EmbeddedDocumentChunk
        chunks = []
        for result in results:
            chunk = EmbeddedDocumentChunk(
                source_uri=result["source_uri"],
                node_path=result["node_path"],
                node_chunk_index=result["node_chunk_index"],
                content=result["content"],
                embedding=result["embedding"],
                modality=result.get("modality", "text"),
                created_at=result.get("created_at"),
            )
            chunks.append(chunk)

        return chunks

    async def search_async(self, query_vector: list[float], limit: int = 10) -> list["EmbeddedDocumentChunk"]:
        """
        Search for similar chunks asynchronously.

        Args:
            query_vector: Query embedding vector
            limit: Maximum number of results to return

        Returns:
            List of similar embedded chunks
        """
        # LanceDB doesn't have native async support yet, so we use the sync method
        return self.search(query_vector, limit)
