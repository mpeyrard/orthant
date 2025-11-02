import datetime as dt
from dataclasses import dataclass
from ..documents import DocumentReader
from ..chunking import ChunkingStrategy
from ..embedding import EmbeddingModel
from ..storage import VectorStore


@dataclass
class EmbeddedDocumentChunk:
    """A document chunk with its embedding vector"""
    source_uri: str
    node_path: str
    node_chunk_index: int
    content: str
    embedding: list[float]
    modality: str = "text"
    created_at: dt.datetime | None = None


class DocumentIngestionPipeline:
    """
    Pipeline that combines document reading, chunking, embedding, and storage.
    Takes a URI and ingests it into the vector store.
    """
    def __init__(
        self,
        reader: DocumentReader,
        chunker: ChunkingStrategy,
        embedder: EmbeddingModel,
        vector_store: VectorStore,
        modality: str = "text",
    ):
        """
        Initialize the ingestion pipeline.
        Args:
            reader: DocumentReader to load documents from URIs
            chunker: ChunkingStrategy to split documents into chunks
            embedder: EmbeddingModel to create vector embeddings
            vector_store: VectorStore for persisting chunks
            modality: Content modality (default: "text")
        """
        self._reader = reader
        self._chunker = chunker
        self._embedder = embedder
        self._vector_store = vector_store
        self._modality = modality

    def ingest(self, file_uri: str) -> list[EmbeddedDocumentChunk]:
        """
        Ingest a document from URI into the vector store.
        Args:
            file_uri: URI of the document to ingest
        Returns:
            List of embedded document chunks that were stored
        """
        document = self._reader.read_file(file_uri)
        chunks = self._chunker.chunk_document(document)
        texts = [chunk.content for chunk in chunks]
        embeddings = self._embedder.encode_batch(texts)
        created_at = dt.datetime.now(dt.timezone.utc)
        embedded_chunks = self._create_embedded_chunks(chunks, embeddings, created_at)
        self._vector_store.store_chunks(embedded_chunks)
        return embedded_chunks

    async def ingest_async(self, file_uri: str) -> list[EmbeddedDocumentChunk]:
        """
        Ingest a document asynchronously into the vector store.
        Args:
            file_uri: URI of the document to ingest
        Returns:
            List of embedded document chunks that were stored
        """
        document = self._reader.read_file(file_uri)
        chunks = self._chunker.chunk_document(document)
        texts = [chunk.content for chunk in chunks]
        embeddings = await self._embedder.encode_batch_async(texts)
        created_at = dt.datetime.now(dt.timezone.utc)
        embedded_chunks = self._create_embedded_chunks(chunks, embeddings, created_at)
        await self._vector_store.store_chunks_async(embedded_chunks)
        return embedded_chunks

    def ingest_batch(self, file_uris: list[str]) -> list[EmbeddedDocumentChunk]:
        """
        Ingest multiple documents into the vector store.
        Args:
            file_uris: List of URIs to ingest
        Returns:
            Flat list of all embedded chunks from all documents
        """
        all_chunks = []
        for uri in file_uris:
            chunks = self.ingest(uri)
            all_chunks.extend(chunks)
        return all_chunks

    async def ingest_batch_async(self, file_uris: list[str]) -> list[EmbeddedDocumentChunk]:
        """
        Ingest multiple documents asynchronously into the vector store.
        Args:
            file_uris: List of URIs to ingest
        Returns:
            Flat list of all embedded chunks from all documents
        """
        all_chunks = []
        for uri in file_uris:
            chunks = await self.ingest_async(uri)
            all_chunks.extend(chunks)
        return all_chunks

    def _create_embedded_chunks(
        self,
        chunks: list,
        embeddings: list[list[float]],
        created_at: dt.datetime,
    ) -> list[EmbeddedDocumentChunk]:
        """Convert chunks and embeddings into EmbeddedDocumentChunk objects."""
        embedded_chunks = []
        for chunk, embedding in zip(chunks, embeddings):
            embedded_chunk = EmbeddedDocumentChunk(
                source_uri=chunk.document_id,
                node_path=chunk.node_path,
                node_chunk_index=chunk.node_chunk_index,
                content=chunk.content,
                embedding=embedding,
                modality=self._modality,
                created_at=created_at,
            )
            embedded_chunks.append(embedded_chunk)
        return embedded_chunks
