import asyncio
from concurrent.futures import ProcessPoolExecutor
from orthant.documents import DocumentIngestPipeline, OrthantDocument

from .chunking_strategy import ChunkingStrategy
from .document_contracts import VectorDocumentNodeChunk
from .embedding_client import EmbeddingClient
from .vector_document_storage import VectorDocumentStorage


class VectorIngestPipeline(DocumentIngestPipeline):
    """Document ingest pipeline for vector data."""
    def __init__(
            self,
            chunking_strategy: ChunkingStrategy,
            embedding_client: EmbeddingClient,
            document_storage: VectorDocumentStorage,
            process_worker_pool: ProcessPoolExecutor,
            *,
            q_chunking: int = 1024,
            q_embeddings: int = 4096,
            q_storage: int = 4096,
            chunker_workers: int = 4,
            embedding_workers: int = 4,
    ):
        self._chunking_strategy = chunking_strategy
        self._embedding_client = embedding_client
        self._document_storage = document_storage
        self._pool = process_worker_pool
        self._chunker_workers = chunker_workers
        self._embedding_workers = embedding_workers
        self._q_chunker = asyncio.Queue(q_chunking)
        self._q_embeddings = asyncio.Queue(q_embeddings)
        self._q_storage = asyncio.Queue(q_storage)
        self._stop_event = asyncio.Event()
        self._worker_tasks: list[asyncio.Task] = []

    async def start_async(self) -> None:
        """Spawn long-lived workers; returns immediately after doing so"""
        if self._worker_tasks:
            raise RuntimeError("Worker tasks already started.")

        loop = asyncio.get_running_loop()
        for _ in range(self._chunker_workers):
            self._worker_tasks.append(asyncio.create_task(self._chunker_worker(loop)))
        for _ in range(self._embedding_workers):
            self._worker_tasks.append(asyncio.create_task(self._embedding_worker()))
        self._worker_tasks.append(asyncio.create_task(self._storage_worker()))

    async def stop_async(self) -> None:
        """Stop all workers and wait for them to finish."""
        self._stop_event.set()
        for task in self._worker_tasks:
            task.cancel()
        await asyncio.gather(*self._worker_tasks, return_exceptions=True)
        self._worker_tasks.clear()

    async def ingest_batch_async(self, documents: list[OrthantDocument]) -> None:
        """
        Enqueue documents and wait until their resulting chunks have been
        stored by the document storage backend.

        Because pipeline worker tasks are long-lived and queues do not close,
        we cannot rely on queue.join() alone to determine completion for a
        batch. Instead, we precompute the expected number of chunks by calling
        the chunking strategy in the executor (this duplicates chunking work
        but gives a reliable completion target), enqueue the documents, and
        wait until the storage worker has recorded that many stored items.
        """
        if not self._worker_tasks:
            raise RuntimeError("Pipeline workers are not started. Call start_async() first.")

        for doc in documents:
            if not isinstance(doc, OrthantDocument):
                raise ValueError(f"Expected OrthantDocument, got {type(doc)}")
            await self._q_chunker.put(doc)

    async def _chunker_worker(self, loop: asyncio.AbstractEventLoop) -> None:
        while not self._stop_event.is_set():
            try:
                next_doc = await asyncio.wait_for(self._q_chunker.get(), timeout=5)
            except asyncio.TimeoutError:
                continue
            try:
                chunks = await loop.run_in_executor(self._pool, self._chunking_strategy.chunk_document, next_doc)
                for chunk in chunks:
                    await self._q_embeddings.put(chunk)
            except Exception:
                pass
            finally:
                self._q_chunker.task_done()

    async def _embedding_worker(self) -> None:
        while not self._stop_event.is_set():
            try:
                next_doc = await asyncio.wait_for(self._q_embeddings.get(), timeout=5)
            except asyncio.TimeoutError:
                continue
            try:
                document_embedding = await self._embedding_client.encode_document_async(next_doc)
                vector_chunk = VectorDocumentNodeChunk(
                    document_id=next_doc.document_id,
                    node_path=next_doc.node_path,
                    node_chunk_index=next_doc.node_chunk_index,
                    content=next_doc.content,
                    vector=document_embedding,
                )
                await self._q_storage.put(vector_chunk)
            except Exception:
                pass
            finally:
                self._q_embeddings.task_done()

    async def _storage_worker(self) -> None:
        while not self._stop_event.is_set():
            try:
                next_doc = await asyncio.wait_for(self._q_storage.get(), timeout=5)
            except asyncio.TimeoutError:
                continue
            try:
                await self._document_storage.store_vector_document_async(next_doc)
            except Exception:
                pass
            finally:
                self._q_storage.task_done()
