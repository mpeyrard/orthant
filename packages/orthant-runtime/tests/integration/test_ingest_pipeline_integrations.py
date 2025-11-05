import pytest
from concurrent.futures import ProcessPoolExecutor

from orthant.embedding import VectorIngestPipeline
from orthant.haystack import HaystackChunkingStrategy
from orthant.mistral import MistralEmbeddingClient
from orthant.documents import OrthantDocument, OrthantDocumentNode


@pytest.mark.integration
class TestIngestPipelineIntegrations:
    @pytest.mark.asyncio
    async def test_haystack_chunker_mistral_embedding_lance_storage(self):
        chunking_strategy = HaystackChunkingStrategy.sentence_splitter(n_sentences=2, overlap=0)
        embedding_client = MistralEmbeddingClient(api_key="...")
        # document_storage = LanceDocumentStorage()
        pipeline = VectorIngestPipeline(
            chunking_strategy=chunking_strategy,
            embedding_client=embedding_client,
            document_storage=None,
            process_worker_pool=ProcessPoolExecutor(max_workers=4),
        )
        await pipeline.start_async()
        doc = OrthantDocument(
            document_id="integration-doc-1",
            source_uri="file://integration/test_doc.txt",
            nodes=[
                OrthantDocumentNode(
                    node_path="/0",
                    content=(
                        "This is the first sentence of the test document. "
                        "It provides enough content so the chunking strategy can split into multiple pieces. "
                        "Here is another sentence to increase length and create additional chunk boundaries for sentence-based chunkers."
                    ),
                ),
                OrthantDocumentNode(
                    node_path="/1",
                    content=(
                        "This second node continues the document and has further sentences. "
                        "It ensures we exercise chunking across multiple nodes and provides overlap test cases. "
                        "A final sentence concludes this node."
                    ),
                ),
            ],
        )

        await pipeline.ingest_batch_async([doc])
