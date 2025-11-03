"""Document parsing protocol.

Defines the `OrthantDocumentParser` protocol used to implement parsers for
various input formats (Markdown, HTML, PDF, etc.).
"""
from typing import Protocol, runtime_checkable
from .document_contracts import OrthantDocument


@runtime_checkable
class OrthantDocumentParser(Protocol):
    """Document parser protocol.

    Implementers should return True from `can_parse` when they are capable of
    parsing the given `file_uri`, and provide a `parse_file` implementation
    that returns a validated `OrthantDocument`.
    """

    def can_parse(self, file_uri: str) -> bool:
        ...

    def parse_file(self, file_uri: str) -> OrthantDocument:
        ...
