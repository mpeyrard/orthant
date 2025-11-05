import pytest
import tempfile

from orthant.lance.document_storage import LanceDocumentStorage
from orthant.embedding import VectorDocumentNodeChunk


@pytest.mark.system
class TestLanceDocStorage:
    @pytest.mark.asyncio
    async def test_store_vector_document(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            uri = tmpdir
            storage = LanceDocumentStorage(uri=uri, table_name="test_chunks", embedding_dim=3)

            # Build a VectorDocumentNodeChunk-like object; use a simple pydantic model
            chunk = VectorDocumentNodeChunk(
                document_id="doc1",
                node_path="/0",
                node_chunk_index=0,
                content="hello world",
                vector=[0.1, 0.2, 0.3],
            )

            await storage.store_vector_document_async(chunk)

            # Re-open table and validate data
            db = storage._db
            table = db.open_table("test_chunks")
            arrow_tbl = table.to_arrow()
            assert arrow_tbl.num_rows == 1
            content = arrow_tbl.column("content").to_pylist()[0]
            emb = arrow_tbl.column("embedding").to_pylist()[0]
            assert content == "hello world"
            assert emb == pytest.approx([0.1, 0.2, 0.3])
