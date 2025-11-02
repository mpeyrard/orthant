from typing import Protocol, runtime_checkable
from .document_contracts import OrthantDocument


@runtime_checkable
class OrthantDocumentParser(Protocol):
    """Document parser protocol"""

    def can_parse(self, file_uri: str) -> bool:
        ...

    def parse_file(self, file_uri: str) -> OrthantDocument:
        ...
