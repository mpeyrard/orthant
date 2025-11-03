"""Tests for LanceDocumentStorage."""
import asyncio
import tempfile
import pytest
from pathlib import Path

from orthant.documents import OrthantDocument, OrthantDocumentNode, OrthantDocumentNodeChunk
from orthant.lance import LanceDocumentStorage


@pytest.mark.asyncio
async def test_store_single_document():
    """Test storing a single document."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = LanceDocumentStorage(
            uri=tmpdir,
            table_name="test_docs",
            embedding_dim=384
        )

        # Create a test document
        doc = OrthantDocument(
            document_id="doc1",
            source_uri="file:///test.txt",
            nodes=[
                OrthantDocumentNode(
                    node_path="/page/1",
                    content="This is test content."
                )
            ]
        )

        # Store the document
        await storage.store_async(doc)

        # Verify table was created
        assert storage._table is not None


@pytest.mark.asyncio
async def test_store_batch_documents():
    """Test storing multiple documents in a batch."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = LanceDocumentStorage(
            uri=tmpdir,
            table_name="test_docs",
            embedding_dim=384
        )

        # Create test documents
        docs = [
            OrthantDocument(
                document_id="doc1",
                source_uri="file:///test1.txt",
                nodes=[
                    OrthantDocumentNode(
                        node_path="/page/1",
                        content="First document content."
                    )
                ]
            ),
            OrthantDocument(
                document_id="doc2",
                source_uri="file:///test2.txt",
                nodes=[
                    OrthantDocumentNode(
                        node_path="/page/1",
                        content="Second document content."
                    )
                ]
            ),
        ]

        # Store the documents
        await storage.store_batch_async(docs)

        # Verify table was created
        assert storage._table is not None


@pytest.mark.asyncio
async def test_store_chunks():
    """Test storing document chunks."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = LanceDocumentStorage(
            uri=tmpdir,
            table_name="test_chunks",
            embedding_dim=384
        )

        # Create test chunks
        chunks = [
            OrthantDocumentNodeChunk(
                document_id="doc1",
                node_path="/page/1",
                node_chunk_index=0,
                content="First chunk content."
            ),
            OrthantDocumentNodeChunk(
                document_id="doc1",
                node_path="/page/1",
                node_chunk_index=1,
                content="Second chunk content."
            ),
        ]

        # Store the chunks
        await storage.store_chunks_async(chunks)

        # Verify table was created
        assert storage._table is not None


@pytest.mark.asyncio
async def test_store_empty_document_list():
    """Test storing an empty list of documents."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = LanceDocumentStorage(
            uri=tmpdir,
            table_name="test_empty",
            embedding_dim=384
        )

        # Store empty list
        await storage.store_batch_async([])

        # Table should not be created for empty list
        assert storage._table is None
