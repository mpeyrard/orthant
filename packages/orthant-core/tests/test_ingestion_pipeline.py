import pytest
import datetime as dt
from pathlib import Path
from unittest.mock import Mock

from orthant.core.ingestion import DocumentIngestionPipeline, EmbeddedDocumentChunk
from orthant.core.documents import (
    TextDocumentReader,
    DefaultContentLoader,
    OrthantDocument,
    OrthantDocumentNode,
    OrthantDocumentNodeChunk,
)


@pytest.fixture
def mock_embedder():
    """Create a mock embedding model"""
    embedder = Mock()
    embedder.encode_batch.return_value = [
        [0.1, 0.2, 0.3],
        [0.4, 0.5, 0.6],
    ]

    async def mock_encode_batch_async(texts):
        return [
            [0.1, 0.2, 0.3],
            [0.4, 0.5, 0.6],
        ]

    embedder.encode_batch_async = mock_encode_batch_async
    return embedder


@pytest.fixture
def mock_chunker():
    """Create a mock chunking strategy"""
    chunker = Mock()
    chunker.chunk_document.return_value = [
        OrthantDocumentNodeChunk(
            document_id="doc1",
            node_path="1",
            node_chunk_index=0,
            content="First chunk content",
        ),
        OrthantDocumentNodeChunk(
            document_id="doc1",
            node_path="1",
            node_chunk_index=1,
            content="Second chunk content",
        ),
    ]
    return chunker


@pytest.fixture
def text_reader():
    """Create a real text document reader"""
    return TextDocumentReader(DefaultContentLoader())


@pytest.fixture
def mock_vector_store():
    """Create a mock vector store"""
    vector_store = Mock()

    async def mock_store_async(chunks):
        pass

    vector_store.store_chunks_async = mock_store_async
    return vector_store


@pytest.mark.unit
class TestDocumentIngestionPipeline:

    def test_init(self, text_reader, mock_chunker, mock_embedder, mock_vector_store):
        """Test pipeline initialization"""
        pipeline = DocumentIngestionPipeline(
            reader=text_reader,
            chunker=mock_chunker,
            embedder=mock_embedder,
            vector_store=mock_vector_store,
        )

        assert pipeline._reader is text_reader
        assert pipeline._chunker is mock_chunker
        assert pipeline._embedder is mock_embedder
        assert pipeline._vector_store is mock_vector_store
        assert pipeline._modality == "text"

    def test_init_with_custom_modality(self, text_reader, mock_chunker, mock_embedder, mock_vector_store):
        """Test pipeline initialization with custom modality"""
        pipeline = DocumentIngestionPipeline(
            reader=text_reader,
            chunker=mock_chunker,
            embedder=mock_embedder,
            vector_store=mock_vector_store,
            modality="markdown",
        )

        assert pipeline._modality == "markdown"

    def test_ingest(self, tmp_path: Path, text_reader, mock_chunker, mock_embedder, mock_vector_store):
        """Test basic document ingestion"""
        # Create a test file
        test_file = tmp_path / "test.txt"
        test_file.write_text("Test content", encoding="utf-8")

        pipeline = DocumentIngestionPipeline(
            reader=text_reader,
            chunker=mock_chunker,
            embedder=mock_embedder,
            vector_store=mock_vector_store,
        )

        chunks = pipeline.ingest(str(test_file))

        # Verify we got embedded chunks
        assert len(chunks) == 2
        assert all(isinstance(c, EmbeddedDocumentChunk) for c in chunks)

        # Verify first chunk
        assert chunks[0].node_path == "1"
        assert chunks[0].node_chunk_index == 0
        assert chunks[0].content == "First chunk content"
        assert chunks[0].embedding == [0.1, 0.2, 0.3]
        assert chunks[0].modality == "text"
        assert chunks[0].created_at is not None

        # Verify second chunk
        assert chunks[1].node_chunk_index == 1
        assert chunks[1].content == "Second chunk content"
        assert chunks[1].embedding == [0.4, 0.5, 0.6]

        # Verify vector store was called
        mock_vector_store.store_chunks.assert_called_once()

    def test_ingest_calls_components_in_order(self, tmp_path: Path, mock_chunker, mock_embedder, mock_vector_store):
        """Test that pipeline calls reader -> chunker -> embedder in sequence"""
        test_file = tmp_path / "test.txt"
        test_file.write_text("Content", encoding="utf-8")

        mock_reader = Mock()
        mock_reader.read_file.return_value = OrthantDocument(
            document_id="doc1",
            source_uri=str(test_file),
            nodes=[OrthantDocumentNode(node_path="1", content="Content")],
        )

        pipeline = DocumentIngestionPipeline(
            reader=mock_reader,
            chunker=mock_chunker,
            embedder=mock_embedder,
            vector_store=mock_vector_store,
        )

        chunks = pipeline.ingest(str(test_file))

        # Verify call order
        mock_reader.read_file.assert_called_once_with(str(test_file))
        mock_chunker.chunk_document.assert_called_once()
        mock_embedder.encode_batch.assert_called_once_with([
            "First chunk content",
            "Second chunk content",
        ])
        mock_vector_store.store_chunks.assert_called_once()

    def test_ingest_preserves_chunk_metadata(self, tmp_path: Path, text_reader, mock_chunker, mock_embedder, mock_vector_store):
        """Test that chunk metadata is preserved in embedded chunks"""
        test_file = tmp_path / "test.txt"
        test_file.write_text("Content", encoding="utf-8")

        pipeline = DocumentIngestionPipeline(
            reader=text_reader,
            chunker=mock_chunker,
            embedder=mock_embedder,
            vector_store=mock_vector_store,
        )

        chunks = pipeline.ingest(str(test_file))

        # Verify metadata preservation
        assert chunks[0].source_uri == "doc1"  # From document_id
        assert chunks[0].node_path == "1"
        assert chunks[0].node_chunk_index == 0

        assert chunks[1].source_uri == "doc1"
        assert chunks[1].node_path == "1"
        assert chunks[1].node_chunk_index == 1

    def test_ingest_adds_timestamps(self, tmp_path: Path, text_reader, mock_chunker, mock_embedder, mock_vector_store):
        """Test that created_at timestamps are added"""
        test_file = tmp_path / "test.txt"
        test_file.write_text("Content", encoding="utf-8")

        pipeline = DocumentIngestionPipeline(
            reader=text_reader,
            chunker=mock_chunker,
            embedder=mock_embedder,
            vector_store=mock_vector_store,
        )

        before = dt.datetime.now(dt.timezone.utc)
        chunks = pipeline.ingest(str(test_file))
        after = dt.datetime.now(dt.timezone.utc)

        # All chunks should have same timestamp within the time window
        for chunk in chunks:
            assert chunk.created_at is not None
            assert before <= chunk.created_at <= after

        # All chunks from the same document should have same timestamp
        assert chunks[0].created_at == chunks[1].created_at

    def test_ingest_with_custom_modality(self, tmp_path: Path, text_reader, mock_chunker, mock_embedder, mock_vector_store):
        """Test that custom modality is applied to chunks"""
        test_file = tmp_path / "test.txt"
        test_file.write_text("Content", encoding="utf-8")

        pipeline = DocumentIngestionPipeline(
            reader=text_reader,
            chunker=mock_chunker,
            embedder=mock_embedder,
            vector_store=mock_vector_store,
            modality="custom",
        )

        chunks = pipeline.ingest(str(test_file))

        assert all(chunk.modality == "custom" for chunk in chunks)

    @pytest.mark.asyncio
    async def test_ingest_async(self, tmp_path: Path, text_reader, mock_chunker, mock_embedder, mock_vector_store):
        """Test async document ingestion"""
        test_file = tmp_path / "test.txt"
        test_file.write_text("Content", encoding="utf-8")

        pipeline = DocumentIngestionPipeline(
            reader=text_reader,
            chunker=mock_chunker,
            embedder=mock_embedder,
            vector_store=mock_vector_store,
        )

        chunks = await pipeline.ingest_async(str(test_file))

        assert len(chunks) == 2
        assert chunks[0].embedding == [0.1, 0.2, 0.3]
        assert chunks[1].embedding == [0.4, 0.5, 0.6]

        # Verify async vector store was called
        mock_vector_store.store_chunks_async.assert_called_once()

    def test_ingest_batch(self, tmp_path: Path, text_reader, mock_chunker, mock_embedder, mock_vector_store):
        """Test batch ingestion of multiple documents"""
        file1 = tmp_path / "test1.txt"
        file1.write_text("Content 1", encoding="utf-8")

        file2 = tmp_path / "test2.txt"
        file2.write_text("Content 2", encoding="utf-8")

        pipeline = DocumentIngestionPipeline(
            reader=text_reader,
            chunker=mock_chunker,
            embedder=mock_embedder,
            vector_store=mock_vector_store,
        )

        chunks = pipeline.ingest_batch([str(file1), str(file2)])

        # Should get chunks from both documents
        assert len(chunks) == 4  # 2 chunks per document

        # Verify vector store was called twice (once per document)
        assert mock_vector_store.store_chunks.call_count == 2

    @pytest.mark.asyncio
    async def test_ingest_batch_async(self, tmp_path: Path, text_reader, mock_chunker, mock_embedder, mock_vector_store):
        """Test async batch ingestion"""
        file1 = tmp_path / "test1.txt"
        file1.write_text("Content 1", encoding="utf-8")

        file2 = tmp_path / "test2.txt"
        file2.write_text("Content 2", encoding="utf-8")

        pipeline = DocumentIngestionPipeline(
            reader=text_reader,
            chunker=mock_chunker,
            embedder=mock_embedder,
            vector_store=mock_vector_store,
        )

        chunks = await pipeline.ingest_batch_async([str(file1), str(file2)])

        assert len(chunks) == 4

        # Verify async vector store was called twice (once per document)
        assert mock_vector_store.store_chunks_async.call_count == 2

    def test_ingest_empty_document(self, tmp_path: Path, text_reader, mock_embedder, mock_vector_store):
        """Test ingesting a document that produces no chunks"""
        test_file = tmp_path / "empty.txt"
        test_file.write_text("", encoding="utf-8")

        mock_chunker = Mock()
        mock_chunker.chunk_document.return_value = []

        pipeline = DocumentIngestionPipeline(
            reader=text_reader,
            chunker=mock_chunker,
            embedder=mock_embedder,
            vector_store=mock_vector_store,
        )

        chunks = pipeline.ingest(str(test_file))

        assert chunks == []
        # Embedder should be called with empty list
        mock_embedder.encode_batch.assert_called_once_with([])
        # Vector store should still be called
        mock_vector_store.store_chunks.assert_called_once_with([])

    def test_embedded_chunk_dataclass(self):
        """Test EmbeddedDocumentChunk dataclass structure"""
        chunk = EmbeddedDocumentChunk(
            source_uri="test.txt",
            node_path="1",
            node_chunk_index=0,
            content="Test content",
            embedding=[0.1, 0.2, 0.3],
        )

        assert chunk.source_uri == "test.txt"
        assert chunk.node_path == "1"
        assert chunk.node_chunk_index == 0
        assert chunk.content == "Test content"
        assert chunk.embedding == [0.1, 0.2, 0.3]
        assert chunk.modality == "text"  # Default value
        assert chunk.created_at is None  # Default value

    def test_embedded_chunk_with_custom_values(self):
        """Test EmbeddedDocumentChunk with custom values"""
        created = dt.datetime(2024, 1, 1, 12, 0, 0, tzinfo=dt.timezone.utc)

        chunk = EmbeddedDocumentChunk(
            source_uri="test.txt",
            node_path="1",
            node_chunk_index=0,
            content="Test",
            embedding=[0.1],
            modality="markdown",
            created_at=created,
        )

        assert chunk.modality == "markdown"
        assert chunk.created_at == created

    def test_ingest_with_data_uri(self, mock_chunker, mock_embedder, mock_vector_store):
        """Test ingesting from data URI"""
        from orthant.core.documents import TextDocumentReader, DefaultContentLoader

        reader = TextDocumentReader(DefaultContentLoader())
        pipeline = DocumentIngestionPipeline(
            reader=reader,
            chunker=mock_chunker,
            embedder=mock_embedder,
            vector_store=mock_vector_store,
        )

        data_uri = "data:,Hello%20World"
        chunks = pipeline.ingest(data_uri)

        assert len(chunks) == 2
        assert all(isinstance(c, EmbeddedDocumentChunk) for c in chunks)
        mock_vector_store.store_chunks.assert_called_once()

    def test_batch_ingestion_handles_errors_gracefully(self, tmp_path: Path, text_reader, mock_chunker, mock_embedder, mock_vector_store):
        """Test that batch ingestion can handle individual file errors"""
        file1 = tmp_path / "test1.txt"
        file1.write_text("Content 1", encoding="utf-8")

        file2 = tmp_path / "nonexistent.txt"

        pipeline = DocumentIngestionPipeline(
            reader=text_reader,
            chunker=mock_chunker,
            embedder=mock_embedder,
            vector_store=mock_vector_store,
        )

        with pytest.raises(FileNotFoundError):
            pipeline.ingest_batch([str(file1), str(file2)])

    def test_pipeline_with_vector_store(self, tmp_path: Path, text_reader, mock_chunker, mock_embedder, mock_vector_store):
        """Test pipeline with vector store integration"""
        test_file = tmp_path / "test.txt"
        test_file.write_text("Content", encoding="utf-8")

        pipeline = DocumentIngestionPipeline(
            reader=text_reader,
            chunker=mock_chunker,
            embedder=mock_embedder,
            vector_store=mock_vector_store,
        )

        chunks = pipeline.ingest(str(test_file))

        mock_vector_store.store_chunks.assert_called_once()
        assert len(chunks) == 2

    @pytest.mark.asyncio
    async def test_pipeline_with_vector_store_async(self, tmp_path: Path, text_reader, mock_chunker, mock_embedder, mock_vector_store):
        """Test async pipeline with vector store"""
        test_file = tmp_path / "test.txt"
        test_file.write_text("Content", encoding="utf-8")

        pipeline = DocumentIngestionPipeline(
            reader=text_reader,
            chunker=mock_chunker,
            embedder=mock_embedder,
            vector_store=mock_vector_store,
        )

        chunks = await pipeline.ingest_async(str(test_file))

        assert len(chunks) == 2
        mock_vector_store.store_chunks_async.assert_called_once()
