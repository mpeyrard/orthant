from uuid import uuid4
from pathlib import Path
from urllib.parse import urlparse

from .content_loader import ContentLoader
from .contracts import OrthantDocument, OrthantDocumentNode, DocumentReader


class TextDocumentReader(DocumentReader):
    """Loads a text file into a `Document`"""

    # Default extensions this reader can handle
    DEFAULT_EXTENSIONS = {".txt", ".text"}

    def __init__(self, content_loader: ContentLoader, extensions: set[str] | None = None):
        self._content_loader = content_loader
        if extensions is None:
            self._extensions = self.DEFAULT_EXTENSIONS
        else:
            self._extensions = {ext.lower() for ext in extensions}

    def can_read(self, file_uri: str) -> bool:
        """Check if this reader can handle the given URI based on file extension or data URI type"""
        try:
            parsed = urlparse(file_uri)
            if parsed.scheme == 'data':
                return self._can_read_data_uri(file_uri)
            path_str = self._extract_path_from_uri(file_uri, parsed)
            extension = Path(path_str).suffix.lower()
            return extension in self._extensions
        except Exception:
            # If we can't parse it, we can't read it
            return False

    @staticmethod
    def _can_read_data_uri(file_uri: str) -> bool:
        """Check if a data URI contains text content"""
        # Data URI format: data:[<mediatype>][;base64],<data>
        data_part = file_uri[5:]  # Skip 'data:'
        if ',' not in data_part:
            return False
        mediatype_part = data_part.split(',')[0]
        # If no mediatype specified, default is text/plain
        if not mediatype_part:
            return True
        # Accept any text/* MIME type
        return 'text/' in mediatype_part

    @staticmethod
    def _extract_path_from_uri(file_uri: str, parsed) -> str:
        """Extract the file path from a URI, handling special cases like zip URIs"""
        # Handle zip URIs (format: zip://inner/file.txt::archive.zip)
        if parsed.scheme == 'zip' and '::' in file_uri:
            return file_uri.split('::')[0].replace('zip://', '')
        # Return the path component or fall back to the full URI
        return parsed.path if parsed.path else file_uri


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
