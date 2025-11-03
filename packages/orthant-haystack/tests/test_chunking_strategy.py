import pytest

from orthant.documents import OrthantDocument, OrthantDocumentNode
from orthant.haystack import HaystackChunkingStrategy


@pytest.mark.unit
class TestChunkingStrategy:
    def test_chunk_by_sentences(self):
        subject = HaystackChunkingStrategy.sentence_splitter(n_sentences=2, overlap=0)
        text = """Orthant ingests CSVs and PDFs.
It builds embeddings for search.

Chunking preserves meaning.
Overlap reduces boundary loss.
This line checks splitting."""

        document = OrthantDocument(
            document_id="test-doc-1",
            source_uri="test://example.txt",
            nodes=[OrthantDocumentNode(node_path="root", content=text)]
        )

        chunks = subject.chunk_document(document)

        # Verify we got chunks back
        assert len(chunks) == 3

        # Verify chunk structure
        assert all(chunk.document_id == "test-doc-1" for chunk in chunks)
        assert all(chunk.node_path == "root" for chunk in chunks)

        # Verify chunk indices are sequential
        assert [chunk.node_chunk_index for chunk in chunks] == [0, 1, 2]

        # Verify chunk content
        norm = lambda s: " ".join(s.split())
        expected = [
            "Orthant ingests CSVs and PDFs. It builds embeddings for search.",
            "Chunking preserves meaning. Overlap reduces boundary loss.",
            "This line checks splitting.",
        ]
        assert [norm(chunk.content) for chunk in chunks] == [norm(e) for e in expected]

    def test_chunk_by_sentences_with_overlap(self):
        subject = HaystackChunkingStrategy.sentence_splitter(n_sentences=6, overlap=2)

        text = """Orthant ingests CSVs and PDFs.
It builds embeddings for search.
The pipeline cleans and splits text.
Sentence windows keep context.
Overlap protects boundary information.
Tests ensure stable behavior.
The retriever prefers compact chunks.
Long documents are processed in batches.
Errors trigger a graceful retry.
Metrics are logged for debugging.
Deployment uses Docker and Kubernetes.
Everything is version controlled in Git."""

        document = OrthantDocument(
            document_id="test-doc-2",
            source_uri="test://example2.txt",
            nodes=[OrthantDocumentNode(node_path="root", content=text)]
        )

        chunks = subject.chunk_document(document)

        expected = [
            (
                "Orthant ingests CSVs and PDFs. "
                "It builds embeddings for search. "
                "The pipeline cleans and splits text. "
                "Sentence windows keep context. "
                "Overlap protects boundary information. "
                "Tests ensure stable behavior."
            ),
            (
                "Overlap protects boundary information. "
                "Tests ensure stable behavior. "
                "The retriever prefers compact chunks. "
                "Long documents are processed in batches. "
                "Errors trigger a graceful retry. "
                "Metrics are logged for debugging."
            ),
            (
                "Errors trigger a graceful retry. "
                "Metrics are logged for debugging. "
                "Deployment uses Docker and Kubernetes. "
                "Everything is version controlled in Git."
            ),
        ]

        norm = lambda s: " ".join(s.split())
        assert [norm(chunk.content) for chunk in chunks] == [norm(e) for e in expected]

        # Verify overlap - last sentences of first chunk match first sentences of second chunk
        assert "Overlap protects boundary information" in chunks[0].content
        assert "Tests ensure stable behavior" in chunks[0].content
        assert chunks[1].content.startswith("Overlap protects boundary information")

    def test_chunk_multiple_nodes(self):
        """Test chunking a document with multiple nodes"""
        subject = HaystackChunkingStrategy.sentence_splitter(n_sentences=2, overlap=0)

        document = OrthantDocument(
            document_id="test-doc-3",
            source_uri="test://multi-node.txt",
            nodes=[
                OrthantDocumentNode(
                    node_path="section1",
                    content="First section has two sentences. This is the second sentence."
                ),
                OrthantDocumentNode(
                    node_path="section2",
                    content="Second section also has sentences. Here is another one. And a third."
                ),
            ]
        )

        chunks = subject.chunk_document(document)

        # Should have chunks from both nodes
        assert len(chunks) >= 2

        # Verify all chunks have correct document_id
        assert all(chunk.document_id == "test-doc-3" for chunk in chunks)

        # Verify chunks from first node
        section1_chunks = [c for c in chunks if c.node_path == "section1"]
        assert len(section1_chunks) == 1
        assert section1_chunks[0].node_chunk_index == 0

        # Verify chunks from second node
        section2_chunks = [c for c in chunks if c.node_path == "section2"]
        assert len(section2_chunks) == 2
        assert [c.node_chunk_index for c in section2_chunks] == [0, 1]

    def test_chunk_empty_document(self):
        """Test chunking a document with no nodes"""
        subject = HaystackChunkingStrategy.sentence_splitter(n_sentences=2, overlap=0)

        document = OrthantDocument(
            document_id="test-doc-empty",
            source_uri="test://empty.txt",
            nodes=[]
        )

        chunks = subject.chunk_document(document)
        assert chunks == []

    def test_chunk_single_sentence_node(self):
        """Test chunking when a node has fewer sentences than the split length"""
        subject = HaystackChunkingStrategy.sentence_splitter(n_sentences=5, overlap=0)

        document = OrthantDocument(
            document_id="test-doc-short",
            source_uri="test://short.txt",
            nodes=[
                OrthantDocumentNode(
                    node_path="short",
                    content="Just one sentence here."
                )
            ]
        )

        chunks = subject.chunk_document(document)

        # Should still create a chunk even though it's shorter than split_length
        assert len(chunks) == 1
        assert chunks[0].node_chunk_index == 0
        assert "Just one sentence here." in chunks[0].content

    def test_chunk_preserves_document_metadata(self):
        """Test that all chunk metadata is correctly preserved"""
        subject = HaystackChunkingStrategy.sentence_splitter(n_sentences=1, overlap=0)

        document = OrthantDocument(
            document_id="doc-with-metadata",
            source_uri="s3://bucket/file.txt",
            nodes=[
                OrthantDocumentNode(
                    node_path="chapter/section/paragraph",
                    content="First sentence. Second sentence. Third sentence."
                )
            ]
        )

        chunks = subject.chunk_document(document)

        assert len(chunks) == 3

        # Verify all metadata is preserved correctly
        for i, chunk in enumerate(chunks):
            assert chunk.document_id == "doc-with-metadata"
            assert chunk.node_path == "chapter/section/paragraph"
            assert chunk.node_chunk_index == i
