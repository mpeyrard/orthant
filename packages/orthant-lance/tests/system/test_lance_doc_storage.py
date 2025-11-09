import pytest
import tempfile
import asyncio

from orthant.lance.document_storage import LanceDocumentStorage, _make_document_chunk_lance_schema
from orthant.embedding import VectorDocumentNodeChunk


@pytest.mark.system
class TestLanceDocStorage:
    @pytest.mark.asyncio
    async def test_store_vector_document(self):
        """Store a single vector chunk and verify written content and embedding."""
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = LanceDocumentStorage(uri=tmpdir, table_name="test_chunks", embedding_dim=3)

            chunk = VectorDocumentNodeChunk(
                document_id="doc1",
                node_path="/0",
                node_chunk_index=0,
                content="hello world",
                vector=[0.1, 0.2, 0.3],
            )

            await storage.store_vector_document_async(chunk)

            table = storage._db.open_table("test_chunks")
            arrow_tbl = table.to_arrow()
            assert arrow_tbl.num_rows == 1
            assert arrow_tbl.column("content").to_pylist()[0] == "hello world"
            assert arrow_tbl.column("embedding").to_pylist()[0] == pytest.approx([0.1, 0.2, 0.3])

    @pytest.mark.asyncio
    async def test_store_multiple_vector_documents(self):
        """Store many vector chunks and ensure counts/distribution are correct."""
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = LanceDocumentStorage(uri=tmpdir, table_name="many_chunks", embedding_dim=4)

            total = 20
            tasks = []
            for i in range(total):
                doc_id = f"doc{i//2}"
                vec = [float(i), float(i + 0.1), float(i + 0.2), float(i + 0.3)]
                chunk = VectorDocumentNodeChunk(document_id=doc_id, node_path="/0", node_chunk_index=i % 2, content=f"c{i}", vector=vec)
                tasks.append(storage.store_vector_document_async(chunk))

            await asyncio.gather(*tasks)

            table = storage._db.open_table("many_chunks")
            arrow_tbl = table.to_arrow()
            assert arrow_tbl.num_rows == total

            ids = arrow_tbl.column("source_uri").to_pylist()
            counts = {}
            for id in ids:
                counts[id] = counts.get(id, 0) + 1
            # since we used doc{i//2} each doc should have 2 chunks
            assert all(c == 2 for c in counts.values())

    @pytest.mark.asyncio
    async def test_vector_dim_mismatch_raises(self):
        """Providing a vector with the wrong dimensionality should raise when writing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = LanceDocumentStorage(uri=tmpdir, table_name="mismatch", embedding_dim=3)

            # vector length is 2 but embedding_dim is 3
            chunk = VectorDocumentNodeChunk(document_id="doc1", node_path="/0", node_chunk_index=0, content="x", vector=[0.1, 0.2])
            with pytest.raises(Exception):
                await storage.store_vector_document_async(chunk)

    def test_table_schema_is_correct(self):
        """Ensure the created table uses the expected schema for the embedding column."""
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = LanceDocumentStorage(uri=tmpdir, table_name="schema", embedding_dim=5)
            # inspect the underlying table schema (pyarrow)
            table = storage._db.open_table("schema")
            arrow_schema = table.to_arrow().schema
            # embedding field should exist and be a list field
            assert "embedding" in {f.name for f in arrow_schema}
            field = arrow_schema.field("embedding")
            # child type should be float32
            child = field.type.value_type if hasattr(field.type, "value_type") else None
            assert child is not None and str(child) in ("float32", "float")

    @pytest.mark.asyncio
    async def test_append_behavior_creates_then_adds(self):
        """First write should create table; subsequent writes should append."""
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = LanceDocumentStorage(uri=tmpdir, table_name="append", embedding_dim=3)
            c1 = VectorDocumentNodeChunk(document_id="d1", node_path="/0", node_chunk_index=0, content="a", vector=[0.1, 0.2, 0.3])
            await storage.store_vector_document_async(c1)
            c2 = VectorDocumentNodeChunk(document_id="d1", node_path="/0", node_chunk_index=1, content="b", vector=[0.4, 0.5, 0.6])
            await storage.store_vector_document_async(c2)

            table = storage._db.open_table("append")
            arrow_tbl = table.to_arrow()
            assert arrow_tbl.num_rows == 2

    @pytest.mark.asyncio
    async def test_created_at_and_source_uri_fallbacks(self):
        """If optional metadata is missing, source_uri falls back to document_id and created_at is set."""
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = LanceDocumentStorage(uri=tmpdir, table_name="fallbacks", embedding_dim=3)
            # omit source_uri and created_at
            chunk = VectorDocumentNodeChunk(document_id="docX", node_path="/0", node_chunk_index=0, content="txt", vector=[0.1, 0.2, 0.3])
            await storage.store_vector_document_async(chunk)

            table = storage._db.open_table("fallbacks")
            arrow_tbl = table.to_arrow()
            row = arrow_tbl.to_pydict()
            # source_uri column contains document_id
            assert row["source_uri"][0] == "docX"
            assert row["created_at"][0] is not None

    @pytest.mark.asyncio
    async def test_concurrent_writes(self):
        """Perform concurrent writes and ensure all records are stored."""
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = LanceDocumentStorage(uri=tmpdir, table_name="concurrent", embedding_dim=3)
            tasks = []
            n = 8
            for i in range(n):
                chunk = VectorDocumentNodeChunk(document_id=f"doc{i}", node_path="/0", node_chunk_index=0, content=str(i), vector=[float(i), float(i), float(i)])
                tasks.append(storage.store_vector_document_async(chunk))
            await asyncio.gather(*tasks)

            table = storage._db.open_table("concurrent")
            assert table.to_arrow().num_rows == n

    @pytest.mark.asyncio
    async def test_roundtrip_precision(self):
        """Verify float vectors round-trip within float32 tolerances."""
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = LanceDocumentStorage(uri=tmpdir, table_name="precision", embedding_dim=3)
            vec = [0.1234567, 0.2345678, 0.3456789]
            chunk = VectorDocumentNodeChunk(document_id="p", node_path="/0", node_chunk_index=0, content="p", vector=vec)
            await storage.store_vector_document_async(chunk)

            table = storage._db.open_table("precision")
            read_vec = table.to_arrow().column("embedding").to_pylist()[0]
            assert read_vec == pytest.approx(vec, rel=1e-6)

    def test_schema_factory_matches(self):
        """Ensure the module-level schema factory produces the expected embedding field length."""
        schema = _make_document_chunk_lance_schema(7)
        field = schema.field("embedding")
        # ensure list child type is float32
        assert str(field.type.value_type) in ("float32", "float")
        # ensure expected length is present (pyarrow list_(..., n))
        assert field.type.list_size == 7
