from .contracts import DocumentReader, OrthantDocument


class DocumentReaderDispatcher(DocumentReader):
    """
    Dispatches document reading to the appropriate DocumentReader based on URI.

    Tries each registered reader in order until one returns True for can_read(),
    then uses that reader to read the document.
    """

    def __init__(self, readers: list[DocumentReader] | None = None):
        """
        Initialize the dispatcher with a list of document readers.

        Args:
            readers: List of DocumentReader instances. Order matters - first match wins.
        """
        self._readers: list[DocumentReader] = readers or []
        # Track readers with their priorities for priority-based registration
        self._priority_readers: list[tuple[int, DocumentReader]] = [
            (0, reader) for reader in self._readers
        ]

    def register_reader(self, reader: DocumentReader, priority: int = 0) -> None:
        """
        Register a document reader with optional priority.

        Args:
            reader: DocumentReader instance to register
            priority: Higher priority readers are tried first (default: 0)
        """
        # Insert at position based on priority (higher priority = earlier in the list)
        insert_pos = len(self._priority_readers)
        for i, (existing_priority, _) in enumerate(self._priority_readers):
            if priority > existing_priority:
                insert_pos = i
                break

        self._priority_readers.insert(insert_pos, (priority, reader))
        self._readers = [r for _, r in self._priority_readers]

    def can_read(self, file_uri: str) -> bool:
        """
        Check if any registered reader can handle the given URI.

        Args:
            file_uri: URI to check

        Returns:
            True if at least one reader can handle the URI, False otherwise
        """
        return any(reader.can_read(file_uri) for reader in self._readers)

    def read_file(self, file_uri: str) -> OrthantDocument:
        """
        Read a document using the first reader that can handle the URI.

        Args:
            file_uri: URI of the document to read

        Returns:
            OrthantDocument loaded by the appropriate reader

        Raises:
            ValueError: If no reader can handle the given URI
        """
        for reader in self._readers:
            if reader.can_read(file_uri):
                return reader.read_file(file_uri)

        raise ValueError(f"No registered reader can handle URI: {file_uri}")
