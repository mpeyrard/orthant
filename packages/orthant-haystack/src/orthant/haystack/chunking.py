"""Haystack-based chunking strategy.

This module provides a `HaystackChunkingStrategy` that adapts Haystack's
`DocumentPreprocessor` to the Orthant `ChunkingStrategy` protocol. It takes
OrthantDocument nodes, converts them to Haystack Documents, runs the
preprocessor, and converts the resulting split documents back into
`OrthantDocumentNodeChunk` objects.

Example:

    # A tiny fake preprocessor for demonstration (real use should provide
    # a configured haystack.components.preprocessors.DocumentPreprocessor)
    class FakeDoc:
        def __init__(self, content):
            self.content = content

    class FakePreprocessor:
        def __init__(self, mapping):
            # mapping: original_content -> list of split contents
            self._mapping = mapping

        def run(self, documents):
            # documents is a list with single haystack Document in our use
            text = documents[0].content
            return {"documents": [FakeDoc(c) for c in self._mapping.get(text, [])]}

    mapping = {"A long paragraph": ["A long", "paragraph"], "Short": ["Short"]}
    strategy = HaystackChunkingStrategy(FakePreprocessor(mapping))
    orthant_doc = OrthantDocument(document_id="doc1", source_uri="file://x", nodes=[...])
    chunks = strategy.chunk_document(orthant_doc)

Note: `sentence_splitter` factory method returns a preconfigured
`HaystackChunkingStrategy` backed by Haystack's `DocumentPreprocessor`.
"""
from haystack import Document
from haystack.components.preprocessors import DocumentPreprocessor

from orthant.documents import OrthantDocument, OrthantDocumentNodeChunk
from orthant.embedding import ChunkingStrategy


class HaystackChunkingStrategy(ChunkingStrategy):
    """ChunkingStrategy implementation powered by Haystack preprocessors.

    The strategy wraps a `DocumentPreprocessor` instance and uses it to split
    node content into smaller passages. Each split passage is returned as an
    `OrthantDocumentNodeChunk` keeping the original document_id and node_path.
    """

    def __init__(self, document_preprocessor: DocumentPreprocessor):
        """Create a HaystackChunkingStrategy.

        Args:
            document_preprocessor: Configured Haystack DocumentPreprocessor to
                use for splitting documents (by sentence, words, or custom
                rules).
        """
        self._document_preprocessor = document_preprocessor

    def chunk_document(self, document: OrthantDocument) -> list[OrthantDocumentNodeChunk]:
        """Split an OrthantDocument into a list of OrthantDocumentNodeChunk.

        For each node in the provided document the node content is converted
        into a Haystack `Document`, processed by the configured preprocessor,
        and each resulting sub-document is mapped back into an
        `OrthantDocumentNodeChunk` with an incrementing `node_chunk_index`.

        Returns:
            List of chunks in the order produced by the preprocessor.
        """
        all_chunks = []
        for node in document.nodes:
            # Convert OrthantDocumentNode to Haystack Document
            haystack_document = Document(content=node.content)
            # Process through Haystack preprocessor
            split_result = self._document_preprocessor.run(documents=[haystack_document])
            split_docs = split_result["documents"]
            # Convert split documents back to OrthantDocumentNodeChunk
            for idx, split_doc in enumerate(split_docs):
                chunk = OrthantDocumentNodeChunk(
                    document_id=document.document_id,
                    node_path=node.node_path,
                    node_chunk_index=idx,
                    content=split_doc.content
                )
                all_chunks.append(chunk)
        return all_chunks

    @staticmethod
    def sentence_splitter(n_sentences: int = 6, overlap: int = 2, **kwargs):
        """Factory that returns a HaystackChunkingStrategy configured to split by sentences.

        Args:
            n_sentences: Number of sentences per split.
            overlap: Number of overlapping sentences between consecutive splits.
            **kwargs: Additional keyword args forwarded to DocumentPreprocessor.

        Returns:
            HaystackChunkingStrategy instance configured with a
            `DocumentPreprocessor` that splits by sentences.
        """
        processor = DocumentPreprocessor(
            split_by="sentence",
            split_length=n_sentences,
            split_overlap=overlap,
            **kwargs
        )
        return HaystackChunkingStrategy(processor)
