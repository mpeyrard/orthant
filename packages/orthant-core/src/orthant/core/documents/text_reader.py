from uuid import uuid4

from .content_loader import ContentLoader
from .contracts import OrthantDocument, OrthantDocumentNode


class TextDocumentReader:
    """Loads a text file into a `Document`"""
    def __init__(self, content_loader: ContentLoader):
        self._content_loader = content_loader

    def read_file(self, file_uri: str) -> OrthantDocument:
        text_content = self._content_loader.load_text(file_uri)
        doc_id = str(uuid4())
        return OrthantDocument(
            document_id=doc_id,
            source_uri=file_uri,
            nodes=[
                OrthantDocumentNode(
                    node_path="1",
                    content=text_content
                )
            ]
        )
