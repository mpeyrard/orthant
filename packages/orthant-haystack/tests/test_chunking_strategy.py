import pytest
from orthant.haystack import HaystackChunkingStrategy


@pytest.mark.unit
class TestChunkingStrategy:
    def test_chunk_by_sentences(self):
        subject = HaystackChunkingStrategy.sentence_splitter(n_sentences=2, overlap=0)
        text = """
        Orthant ingests CSVs and PDFs.
        It builds embeddings for search.

        Chunking preserves meaning.
        Overlap reduces boundary loss.
        This line checks splitting.
        """
        chunks = subject.split_text(text=text)
        expected = [
            "Orthant ingests CSVs and PDFs. It builds embeddings for search.",
            "Chunking preserves meaning. Overlap reduces boundary loss.",
            "This line checks splitting.",
        ]
        assert [c.strip() for c in chunks] == expected
        assert len(chunks) == 3
        assert all(c.endswith(('.', '!', '?')) for c in chunks)

    def test_chunk_by_sentences_with_overlap(self):
        subject = HaystackChunkingStrategy.sentence_splitter(n_sentences=6, overlap=2)

        text = """
        Orthant ingests CSVs and PDFs.
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
        Everything is version controlled in Git.
        """

        chunks = subject.split_text(text=text)

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
        assert [norm(c) for c in chunks] == [norm(e) for e in expected]
        assert expected[0].endswith(
            "Overlap protects boundary information. Tests ensure stable behavior."
        )
        assert expected[1].startswith(
            "Overlap protects boundary information. Tests ensure stable behavior."
        )
