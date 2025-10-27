from typing import IO
from uuid import uuid4

from .contracts import Document, TextDocumentNode


class TextDocumentReader:
    def read_file(self, file: IO[bytes] | IO[str]) -> Document:
        doc_id = str(uuid4())
        return Document(
            document_id=doc_id,
            text_nodes=[
                TextDocumentNode(
                    node_path=doc_id,
                    content=file.read()
                )
            ]
        )
