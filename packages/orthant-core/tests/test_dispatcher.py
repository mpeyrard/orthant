import pytest
from pathlib import Path
from unittest.mock import Mock

from orthant.core.documents import (
    DocumentReaderDispatcher,
    TextDocumentReader,
    DefaultContentLoader,
    OrthantDocument,
    OrthantDocumentNode,
)
from orthant.core.documents.contracts import DocumentReader


@pytest.fixture
def text_reader():
    """Create a TextDocumentReader for testing"""
    return TextDocumentReader(DefaultContentLoader())


@pytest.fixture
def mock_pdf_reader():
    """Create a mock PDF reader for testing"""
    reader = Mock(spec=DocumentReader)
    reader.can_read.side_effect = lambda uri: uri.endswith('.pdf')
    reader.read_file.return_value = OrthantDocument(
        document_id="pdf-doc",
        source_uri="test.pdf",
        nodes=[OrthantDocumentNode(node_path="1", content="PDF content")]
    )
    return reader


@pytest.fixture
def mock_html_reader():
    """Create a mock HTML reader for testing"""
    reader = Mock(spec=DocumentReader)
    reader.can_read.side_effect = lambda uri: uri.endswith('.html')
    reader.read_file.return_value = OrthantDocument(
        document_id="html-doc",
        source_uri="test.html",
        nodes=[OrthantDocumentNode(node_path="1", content="HTML content")]
    )
    return reader


@pytest.mark.unit
class TestDocumentReaderDispatcher:

    def test_init_empty(self):
        """Test creating dispatcher with no readers"""
        dispatcher = DocumentReaderDispatcher()
        assert dispatcher.can_read("any.txt") == False

    def test_init_with_readers(self, text_reader, mock_pdf_reader):
        """Test creating dispatcher with initial readers"""
        dispatcher = DocumentReaderDispatcher([text_reader, mock_pdf_reader])
        assert dispatcher.can_read("test.txt") == True
        assert dispatcher.can_read("test.pdf") == True

    def test_register_reader(self, text_reader):
        """Test registering a reader"""
        dispatcher = DocumentReaderDispatcher()
        assert dispatcher.can_read("test.txt") == False

        dispatcher.register_reader(text_reader)
        assert dispatcher.can_read("test.txt") == True

    def test_register_multiple_readers(self, text_reader, mock_pdf_reader, mock_html_reader):
        """Test registering multiple readers"""
        dispatcher = DocumentReaderDispatcher()
        dispatcher.register_reader(text_reader)
        dispatcher.register_reader(mock_pdf_reader)
        dispatcher.register_reader(mock_html_reader)

        assert dispatcher.can_read("test.txt") == True
        assert dispatcher.can_read("test.pdf") == True
        assert dispatcher.can_read("test.html") == True

    def test_register_reader_with_priority(self, tmp_path: Path):
        """Test that priority affects reader order"""
        dispatcher = DocumentReaderDispatcher()

        # Create two readers that both handle .txt files
        reader_low = Mock(spec=DocumentReader)
        reader_low.can_read.return_value = True
        reader_low.read_file.return_value = OrthantDocument(
            document_id="low",
            source_uri="test.txt",
            nodes=[OrthantDocumentNode(node_path="1", content="Low priority")]
        )

        reader_high = Mock(spec=DocumentReader)
        reader_high.can_read.return_value = True
        reader_high.read_file.return_value = OrthantDocument(
            document_id="high",
            source_uri="test.txt",
            nodes=[OrthantDocumentNode(node_path="1", content="High priority")]
        )

        dispatcher.register_reader(reader_low, priority=0)
        dispatcher.register_reader(reader_high, priority=10)

        # Higher priority reader should be used first
        doc = dispatcher.read_file("test.txt")
        assert doc.nodes[0].content == "High priority"

    def test_register_reader_priority_ordering(self):
        """Test that readers are ordered by priority (highest first)"""
        dispatcher = DocumentReaderDispatcher()

        reader1 = Mock(spec=DocumentReader)
        reader1.can_read.return_value = True
        reader1.read_file.return_value = OrthantDocument(
            document_id="1",
            source_uri="test.txt",
            nodes=[OrthantDocumentNode(node_path="1", content="Reader 1")]
        )

        reader2 = Mock(spec=DocumentReader)
        reader2.can_read.return_value = True
        reader2.read_file.return_value = OrthantDocument(
            document_id="2",
            source_uri="test.txt",
            nodes=[OrthantDocumentNode(node_path="1", content="Reader 2")]
        )

        reader3 = Mock(spec=DocumentReader)
        reader3.can_read.return_value = True
        reader3.read_file.return_value = OrthantDocument(
            document_id="3",
            source_uri="test.txt",
            nodes=[OrthantDocumentNode(node_path="1", content="Reader 3")]
        )

        dispatcher.register_reader(reader1, priority=5)
        dispatcher.register_reader(reader2, priority=10)
        dispatcher.register_reader(reader3, priority=1)

        # Reader with priority 10 should be used first
        doc = dispatcher.read_file("test.txt")
        assert doc.nodes[0].content == "Reader 2"

    def test_can_read_with_matching_reader(self, text_reader):
        """Test can_read returns True when a reader can handle the URI"""
        dispatcher = DocumentReaderDispatcher([text_reader])
        assert dispatcher.can_read("document.txt") == True

    def test_can_read_with_no_matching_reader(self, text_reader):
        """Test can_read returns False when no reader can handle the URI"""
        dispatcher = DocumentReaderDispatcher([text_reader])
        assert dispatcher.can_read("document.pdf") == False

    def test_can_read_empty_dispatcher(self):
        """Test can_read returns False when no readers are registered"""
        dispatcher = DocumentReaderDispatcher()
        assert dispatcher.can_read("any.txt") == False

    def test_can_read_with_multiple_readers(self, text_reader, mock_pdf_reader):
        """Test can_read with multiple readers"""
        dispatcher = DocumentReaderDispatcher([text_reader, mock_pdf_reader])
        assert dispatcher.can_read("document.txt") == True
        assert dispatcher.can_read("document.pdf") == True
        assert dispatcher.can_read("document.docx") == False

    def test_read_file_dispatches_to_correct_reader(self, tmp_path: Path, text_reader):
        """Test that read_file uses the correct reader"""
        p = tmp_path / "test.txt"
        p.write_text("Hello from text file", encoding="utf-8")

        dispatcher = DocumentReaderDispatcher([text_reader])
        doc = dispatcher.read_file(str(p))

        assert doc.source_uri == str(p)
        assert doc.nodes[0].content == "Hello from text file"

    def test_read_file_uses_first_matching_reader(self, tmp_path: Path):
        """Test that first matching reader is used when multiple can handle the URI"""
        p = tmp_path / "test.txt"
        p.write_text("Content", encoding="utf-8")

        reader1 = Mock(spec=DocumentReader)
        reader1.can_read.return_value = True
        reader1.read_file.return_value = OrthantDocument(
            document_id="doc1",
            source_uri=str(p),
            nodes=[OrthantDocumentNode(node_path="1", content="Reader 1")]
        )

        reader2 = Mock(spec=DocumentReader)
        reader2.can_read.return_value = True
        reader2.read_file.return_value = OrthantDocument(
            document_id="doc2",
            source_uri=str(p),
            nodes=[OrthantDocumentNode(node_path="1", content="Reader 2")]
        )

        dispatcher = DocumentReaderDispatcher([reader1, reader2])
        doc = dispatcher.read_file(str(p))

        # Should use reader1 (first match)
        assert doc.nodes[0].content == "Reader 1"
        reader1.read_file.assert_called_once_with(str(p))
        reader2.read_file.assert_not_called()

    def test_read_file_raises_on_no_matching_reader(self):
        """Test that read_file raises ValueError when no reader can handle the URI"""
        dispatcher = DocumentReaderDispatcher()

        with pytest.raises(ValueError, match="No registered reader can handle URI"):
            dispatcher.read_file("unknown.xyz")

    def test_read_file_with_multiple_readers(self, tmp_path: Path, text_reader, mock_pdf_reader):
        """Test reading different file types with appropriate readers"""
        txt_file = tmp_path / "test.txt"
        txt_file.write_text("Text content", encoding="utf-8")

        dispatcher = DocumentReaderDispatcher([text_reader, mock_pdf_reader])

        # Read text file
        doc = dispatcher.read_file(str(txt_file))
        assert doc.nodes[0].content == "Text content"

        # Read PDF file (mocked)
        doc_pdf = dispatcher.read_file("document.pdf")
        assert doc_pdf.nodes[0].content == "PDF content"

    def test_dispatcher_implements_document_reader_protocol(self):
        """Test that dispatcher implements DocumentReader protocol"""
        dispatcher = DocumentReaderDispatcher()
        assert isinstance(dispatcher, DocumentReader)

    def test_dispatcher_can_be_nested(self, text_reader, mock_pdf_reader):
        """Test that a dispatcher can be registered with another dispatcher"""
        inner_dispatcher = DocumentReaderDispatcher([text_reader])
        outer_dispatcher = DocumentReaderDispatcher([mock_pdf_reader])
        outer_dispatcher.register_reader(inner_dispatcher)

        # Outer dispatcher should handle both PDF and TXT
        assert outer_dispatcher.can_read("test.pdf") == True
        assert outer_dispatcher.can_read("test.txt") == True

    def test_read_file_with_data_uri(self, text_reader):
        """Test reading from data URI"""
        dispatcher = DocumentReaderDispatcher([text_reader])

        data_uri = "data:text/plain,Hello%20World"
        doc = dispatcher.read_file(data_uri)

        assert doc.source_uri == data_uri
        assert "Hello World" in doc.nodes[0].content

    def test_priority_with_same_extension(self):
        """Test that priority determines which reader handles files when both can read"""
        reader_low = Mock(spec=DocumentReader)
        reader_low.can_read.return_value = True
        reader_low.read_file.return_value = OrthantDocument(
            document_id="low",
            source_uri="test.txt",
            nodes=[OrthantDocumentNode(node_path="1", content="Low priority")]
        )

        reader_high = Mock(spec=DocumentReader)
        reader_high.can_read.return_value = True
        reader_high.read_file.return_value = OrthantDocument(
            document_id="high",
            source_uri="test.txt",
            nodes=[OrthantDocumentNode(node_path="1", content="High priority")]
        )

        dispatcher = DocumentReaderDispatcher()
        dispatcher.register_reader(reader_low, priority=1)
        dispatcher.register_reader(reader_high, priority=10)

        doc = dispatcher.read_file("test.txt")
        assert doc.nodes[0].content == "High priority"

    def test_register_reader_after_init(self, text_reader, mock_pdf_reader):
        """Test registering readers after initialization"""
        dispatcher = DocumentReaderDispatcher([text_reader])
        assert dispatcher.can_read("test.pdf") == False

        dispatcher.register_reader(mock_pdf_reader)
        assert dispatcher.can_read("test.pdf") == True

    def test_error_message_includes_uri(self):
        """Test that error message includes the problematic URI"""
        dispatcher = DocumentReaderDispatcher()

        with pytest.raises(ValueError) as exc_info:
            dispatcher.read_file("path/to/unknown.xyz")

        assert "path/to/unknown.xyz" in str(exc_info.value)

    def test_can_read_called_in_order(self):
        """Test that can_read is called on readers in priority order"""
        reader1 = Mock(spec=DocumentReader)
        reader1.can_read.return_value = False

        reader2 = Mock(spec=DocumentReader)
        reader2.can_read.return_value = True
        reader2.read_file.return_value = OrthantDocument(
            document_id="doc",
            source_uri="test.txt",
            nodes=[OrthantDocumentNode(node_path="1", content="Content")]
        )

        reader3 = Mock(spec=DocumentReader)
        reader3.can_read.return_value = True

        dispatcher = DocumentReaderDispatcher([reader1, reader2, reader3])
        dispatcher.read_file("test.txt")

        # reader1 and reader2 should be called, but not reader3
        reader1.can_read.assert_called_once_with("test.txt")
        reader2.can_read.assert_called_once_with("test.txt")
        reader3.can_read.assert_not_called()  # Should stop at reader2
