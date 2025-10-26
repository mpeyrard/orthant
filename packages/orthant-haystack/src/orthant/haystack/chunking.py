from haystack import Document
from haystack.components.preprocessors import DocumentPreprocessor

from orthant.core import ChunkingStrategy

class HaystackChunkingStrategy(ChunkingStrategy):
    def __init__(self, document_preprocessor: DocumentPreprocessor):
        self._document_preprocessor = document_preprocessor

    def split_text(self, text: str) -> list[str]:
        haystack_document = Document(content=text)
        split_docs = self._document_preprocessor.run(documents=[haystack_document])
        return [d.content for d in split_docs["documents"]]

    @staticmethod
    def sentence_splitter(n_sentences: int = 6, overlap: int = 2, **kwargs):
        processor = DocumentPreprocessor(
            split_by="sentence",
            split_length=n_sentences,
            split_overlap=overlap,
            **kwargs
        )
        return HaystackChunkingStrategy(processor)
