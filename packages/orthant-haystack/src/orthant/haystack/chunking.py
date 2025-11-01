from haystack import Document
from haystack.components.preprocessors import DocumentPreprocessor

from orthant.core import ChunkingStrategy
from orthant.core.documents import OrthantDocument, OrthantDocumentNodeChunk

class HaystackChunkingStrategy(ChunkingStrategy):
    def __init__(self, document_preprocessor: DocumentPreprocessor):
        self._document_preprocessor = document_preprocessor

    def chunk_document(self, document: OrthantDocument) -> list[OrthantDocumentNodeChunk]:
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
        processor = DocumentPreprocessor(
            split_by="sentence",
            split_length=n_sentences,
            split_overlap=overlap,
            **kwargs
        )
        return HaystackChunkingStrategy(processor)
