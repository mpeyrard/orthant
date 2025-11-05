import asyncio
import warnings
import multiprocessing
import pytest
from unittest.mock import MagicMock
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

# Suppress known lancedev/lance multiprocessing warning when running tests with fork/spawn
warnings.filterwarnings("ignore", message=r"lance is not fork-safe.*")

from orthant.embedding import VectorIngestPipeline, ChunkingStrategy
from orthant.documents import OrthantDocument, OrthantDocumentNode, OrthantDocumentNodeChunk


class SimpleChunker:
    def chunk_document(self, document: OrthantDocument) -> list[OrthantDocumentNodeChunk]:
        chunks = []
        for node_idx, node in enumerate(document.nodes):
            chunks.append(OrthantDocumentNodeChunk(
                document_id=document.document_id,
                node_path=node.node_path,
                node_chunk_index=0,
                content=node.content,
            ))
        return chunks


class SimpleEmbedder:
    async def encode_document_async(self, document_chunk: OrthantDocumentNodeChunk):
        # deterministic embedding for test: length of content as a single-dim vector
        return [float(len(document_chunk.content))]


class RecordingStorage:
    def __init__(self):
        self.stored = []

    async def store_vector_document_async(self, document_with_embedding):
        self.stored.append(document_with_embedding)


@pytest.mark.unit
class TestVectorIngestPipeline:
    def test_init(self):
        # Create mocks for all dependencies
        mock_chunking = MagicMock(name="chunking_strategy")
        mock_embedding = MagicMock(name="embedding_client")
        mock_storage = MagicMock(name="document_storage")
        # Use an autospec-like mock for the process pool to match the expected API
        mock_pool = MagicMock(spec=ThreadPoolExecutor, name="process_pool")

        # Construct pipeline: try keyword args first (clearer), fall back to positional
        try:
            pipeline = VectorIngestPipeline(
                chunking_strategy=mock_chunking,
                embedding_client=mock_embedding,
                document_storage=mock_storage,
                process_worker_pool=mock_pool,
            )
        except TypeError:
            pipeline = VectorIngestPipeline(mock_chunking, mock_embedding, mock_storage, mock_pool)

        # Basic sanity checks
        assert isinstance(pipeline, VectorIngestPipeline)
        # Ensure the constructor didn't call collaborators during initialization
        mock_chunking.assert_not_called()
        mock_embedding.assert_not_called()
        mock_storage.assert_not_called()
        mock_pool.assert_not_called()

    @pytest.mark.asyncio
    async def test_start_and_stop_async(self):
        import asyncio
        # Create mocks for dependencies
        mock_chunking = MagicMock(name="chunking_strategy")
        mock_embedding = MagicMock(name="embedding_client")
        mock_storage = MagicMock(name="document_storage")

        # Provide an async coroutine on the storage mock to be safe if called
        async def _dummy_store(x):
            return None
        mock_storage.store_vector_document_async = _dummy_store

        # Real process pool executor
        pool = ThreadPoolExecutor(max_workers=1)

        try:
            # Construct pipeline
            try:
                pipeline = VectorIngestPipeline(
                    chunking_strategy=mock_chunking,
                    embedding_client=mock_embedding,
                    document_storage=mock_storage,
                    process_worker_pool=pool,
                    chunker_workers=1,
                    embedding_workers=1,
                )
            except TypeError:
                pipeline = VectorIngestPipeline(mock_chunking, mock_embedding, mock_storage, pool)

            # Replace the internal storage queue with a simple mock so task_done is a no-op
            pipeline._q_storage = MagicMock()

            # Start workers
            await pipeline.start_async()

            # Allow a tiny moment for tasks to be created
            await asyncio.sleep(0.05)

            # Expect worker tasks: chunker_workers + embedding_workers + storage worker
            expected = pipeline._chunker_workers + pipeline._embedding_workers + 1
            assert len(pipeline._worker_tasks) == expected
            assert all(isinstance(t, asyncio.Task) for t in pipeline._worker_tasks)

            # Stop workers and ensure tasks are cleared
            await pipeline.stop_async()
            assert pipeline._worker_tasks == []

            # Ensure mocks were not invoked during start/stop
            mock_chunking.assert_not_called()
            mock_embedding.assert_not_called()
        finally:
            pool.shutdown(wait=True)

    @pytest.mark.asyncio
    async def test_start_async_cannot_be_called_twice(self):
        import asyncio
        # Create mocks for dependencies
        mock_chunking = MagicMock(name="chunking_strategy")
        mock_embedding = MagicMock(name="embedding_client")
        mock_storage = MagicMock(name="document_storage")

        # Provide an async coroutine on the storage mock to be safe if called
        async def _dummy_store(x):
            return None
        mock_storage.store_vector_document_async = _dummy_store

        # Real process pool executor
        pool = ThreadPoolExecutor(max_workers=1)

        try:
            # Construct pipeline (support both kw and positional constructors)
            try:
                pipeline = VectorIngestPipeline(
                    chunking_strategy=mock_chunking,
                    embedding_client=mock_embedding,
                    document_storage=mock_storage,
                    process_worker_pool=pool,
                    chunker_workers=1,
                    embedding_workers=1,
                )
            except TypeError:
                pipeline = VectorIngestPipeline(mock_chunking, mock_embedding, mock_storage, pool)

            # Replace the internal storage queue with a simple mock, so task_done is a no-op
            pipeline._q_storage = MagicMock()

            # The first start should succeed
            await pipeline.start_async()

            # The second start should raise RuntimeError
            with pytest.raises(RuntimeError):
                await pipeline.start_async()

            # Stop to clean up
            await pipeline.stop_async()
        finally:
            pool.shutdown(wait=True)

    @pytest.mark.asyncio
    async def test_stop_async_clears_workers_and_is_idempotent(self):
        import asyncio
        # Create mocks for dependencies
        mock_chunking = MagicMock(name="chunking_strategy")
        mock_embedding = MagicMock(name="embedding_client")
        mock_storage = MagicMock(name="document_storage")

        # Provide an async coroutine on the storage mock to be safe if called
        async def _dummy_store(x):
            return None
        mock_storage.store_vector_document_async = _dummy_store

        # Real process pool executor
        pool = ThreadPoolExecutor(max_workers=1)

        try:
            # Construct pipeline
            try:
                pipeline = VectorIngestPipeline(
                    chunking_strategy=mock_chunking,
                    embedding_client=mock_embedding,
                    document_storage=mock_storage,
                    process_worker_pool=pool,
                    chunker_workers=1,
                    embedding_workers=1,
                )
            except TypeError:
                pipeline = VectorIngestPipeline(mock_chunking, mock_embedding, mock_storage, pool)

            # Replace internal storage queue
            pipeline._q_storage = MagicMock()

            # Start and then stop the pipeline
            await pipeline.start_async()
            # give tasks a moment to start
            await asyncio.sleep(0.02)

            await pipeline.stop_async()
            # After stopping, worker tasks should be cleared
            assert pipeline._worker_tasks == []

            # Calling stop_async again should not raise and should leave tasks cleared
            await pipeline.stop_async()
            assert pipeline._worker_tasks == []
        finally:
            pool.shutdown(wait=True)

    @pytest.mark.parametrize("processes", [1, 2, 4, 8])
    @pytest.mark.asyncio
    async def test_full_pipeline_flow_stores_vectors(self, processes):
        # Create 20 documents, each with 2 nodes, so each produces 2 chunks/vectors
        docs = []
        for i in range(20):
            docs.append(OrthantDocument(
                document_id=f"doc{i}",
                source_uri=f"file://test/{i}",
                nodes=[
                    OrthantDocumentNode(node_path="/0", content=f"hello world {i}-a"),
                    OrthantDocumentNode(node_path="/1", content=f"hello world {i}-b"),
                ]
            ))

        # Use spawn context to avoid forking a multithreaded pytest process
        ctx = multiprocessing.get_context("spawn")
        pool = ProcessPoolExecutor(max_workers=processes, mp_context=ctx)
        try:
            chunker = SimpleChunker()
            embedder = SimpleEmbedder()

            # Use a storage mock implemented as an async function that records calls
            stored = []
            async def _mock_store(x):
                stored.append(x)

            mock_storage = MagicMock(name="document_storage")
            mock_storage.store_vector_document_async = _mock_store

            pipeline = VectorIngestPipeline(chunker, embedder, mock_storage, pool, chunker_workers=1, embedding_workers=1)

            await pipeline.start_async()
            await pipeline.ingest_batch_async(docs)
            expected_total = len(docs) * 2

            async def _wait_for_stored(timeout=5.0):
                end = asyncio.get_event_loop().time() + timeout
                while asyncio.get_event_loop().time() < end:
                    if len(stored) >= expected_total:
                        return True
                    await asyncio.sleep(0.01)
                return False

            assert await _wait_for_stored(), f"expected {expected_total} stored items but got {len(stored)}"

            # Validate distribution: each document should have produced exactly 2 stored chunks
            assert len(stored) == expected_total
            counts = {}
            for chunk in stored:
                counts.setdefault(chunk.document_id, 0)
                counts[chunk.document_id] += 1
            assert len(counts) == len(docs)
            assert all(c == 2 for c in counts.values())
            await pipeline.stop_async()
        finally:
            pool.shutdown(wait=True)
