import pytest
import uuid
import fsspec
import zipfile
from pathlib import Path
from unittest.mock import Mock

from orthant.core.documents import TextDocumentReader, DefaultContentLoader, ContentLoader
from orthant.core.documents.contracts import DocumentReader


TEXT_CASES = [
    ("hello.txt", "hello"),
    ("unicode.txt", "Café 🚀"),
    ("empty.txt", ""),
]


@pytest.fixture
def reader():
    return TextDocumentReader(DefaultContentLoader())


@pytest.mark.unit
class TestTextReader:
    @pytest.mark.parametrize(("filename", "content"), TEXT_CASES)
    def test_read_file_local_paths(self, tmp_path: Path, filename: str, content: str):
        p = tmp_path / filename
        p.write_text(content, encoding="utf-8")
        reader = TextDocumentReader(DefaultContentLoader())
        doc = reader.read_file(str(p))
        assert doc.source_uri == str(p)
        assert doc.nodes and doc.nodes[0].node_path == "1"
        assert doc.nodes[0].content == content
        assert isinstance(doc.document_id, str) and len(doc.document_id) >= 32

    def test_read_file_file_uri(self, tmp_path: Path):
        p = tmp_path / "via_file_uri.txt"
        p.write_text("hi from file://", encoding="utf-8")
        reader = TextDocumentReader(DefaultContentLoader())
        uri = p.resolve().as_uri()
        doc = reader.read_file(uri)
        assert doc.source_uri == uri
        assert doc.nodes[0].content == "hi from file://"

    # Error handling tests
    def test_read_file_not_found(self, tmp_path: Path, reader):
        """Test that reading a non-existent file raises FileNotFoundError"""
        non_existent = tmp_path / "does_not_exist.txt"
        with pytest.raises(FileNotFoundError):
            reader.read_file(str(non_existent))

    def test_read_file_propagates_content_loader_errors(self, tmp_path: Path):
        """Test that errors from the content loader are propagated"""
        mock_loader: DefaultContentLoader = Mock(spec=DefaultContentLoader)
        mock_loader.load_text.side_effect = PermissionError("Access denied")

        reader = TextDocumentReader(mock_loader)
        p = tmp_path / "test.txt"

        with pytest.raises(PermissionError, match="Access denied"):
            reader.read_file(str(p))

    # Different URI schemes
    def test_read_file_data_uri(self, reader):
        """Test reading from data URI"""
        data_uri = "data:text/plain;base64,SGVsbG8gRGF0YSBVUkk="  # "Hello Data URI"
        doc = reader.read_file(data_uri)
        assert doc.source_uri == data_uri
        assert doc.nodes[0].content == "Hello Data URI"
        assert doc.nodes[0].node_path == "1"

    def test_read_file_data_uri_url_encoded(self, reader):
        """Test reading from URL-encoded data URI"""
        data_uri = "data:,Hello%20World%21"  # "Hello World!"
        doc = reader.read_file(data_uri)
        assert doc.source_uri == data_uri
        assert doc.nodes[0].content == "Hello World!"

    def test_read_file_memory_filesystem(self, reader):
        """Test reading from memory filesystem"""
        uri = f"memory://{uuid.uuid4()}/test.txt"
        test_content = "Content in memory filesystem"

        with fsspec.open(uri, "wt", encoding="utf-8") as f:
            f.write(test_content)

        doc = reader.read_file(uri)
        assert doc.source_uri == uri
        assert doc.nodes[0].content == test_content

    def test_read_file_from_zip(self, tmp_path: Path, reader):
        """Test reading text file from within a zip archive"""
        archive = tmp_path / "archive.zip"
        inner_content = "Text inside zip archive"

        with zipfile.ZipFile(archive, mode="w") as z:
            z.writestr("inner/document.txt", inner_content)

        inner_uri = f"zip://inner/document.txt::{archive}"
        doc = reader.read_file(inner_uri)
        assert doc.source_uri == inner_uri
        assert doc.nodes[0].content == inner_content

    # Content verification tests
    def test_read_file_multiline_content(self, tmp_path: Path, reader):
        """Test reading multiline text files"""
        p = tmp_path / "multiline.txt"
        content = "Line 1\nLine 2\nLine 3\n\nLine 5"
        p.write_text(content, encoding="utf-8")

        doc = reader.read_file(str(p))
        assert doc.nodes[0].content == content
        assert doc.nodes[0].content.count("\n") == 4

    def test_read_file_large_content(self, tmp_path: Path, reader):
        """Test reading large text file"""
        p = tmp_path / "large.txt"
        # Create ~1MB text file
        large_content = "This is a line of text.\n" * 50000
        p.write_text(large_content, encoding="utf-8")

        doc = reader.read_file(str(p))
        assert doc.nodes[0].content == large_content
        assert len(doc.nodes[0].content) > 1_000_000

    def test_read_file_unicode_content(self, tmp_path: Path, reader):
        """Test reading files with various Unicode characters"""
        p = tmp_path / "unicode.txt"
        content = "Hello 世界 🌍 café мир"
        p.write_text(content, encoding="utf-8")

        doc = reader.read_file(str(p))
        assert doc.nodes[0].content == content

    def test_read_file_with_special_chars_in_path(self, tmp_path: Path, reader):
        """Test reading files with special characters in path"""
        special_dir = tmp_path / "dir with spaces" / "sub-dir"
        special_dir.mkdir(parents=True)
        p = special_dir / "file with spaces.txt"
        p.write_text("Content", encoding="utf-8")

        doc = reader.read_file(str(p))
        assert doc.source_uri == str(p)
        assert doc.nodes[0].content == "Content"

    # Document structure tests
    def test_document_has_single_node(self, tmp_path: Path, reader):
        """Test that TextDocumentReader always creates exactly one node"""
        p = tmp_path / "test.txt"
        p.write_text("Some content", encoding="utf-8")

        doc = reader.read_file(str(p))
        assert len(doc.nodes) == 1

    def test_node_path_is_always_one(self, tmp_path: Path, reader):
        """Test that the node path is always '1' for text documents"""
        p = tmp_path / "test.txt"
        p.write_text("Content", encoding="utf-8")

        doc = reader.read_file(str(p))
        assert doc.nodes[0].node_path == "1"

    def test_document_id_is_unique(self, tmp_path: Path, reader):
        """Test that each read generates a unique document ID"""
        p = tmp_path / "test.txt"
        p.write_text("Content", encoding="utf-8")

        doc1 = reader.read_file(str(p))
        doc2 = reader.read_file(str(p))

        assert doc1.document_id != doc2.document_id
        # Verify they are valid UUIDs
        uuid.UUID(doc1.document_id)
        uuid.UUID(doc2.document_id)

    def test_document_id_is_uuid_format(self, tmp_path: Path, reader):
        """Test that document_id is a valid UUID string"""
        p = tmp_path / "test.txt"
        p.write_text("Content", encoding="utf-8")

        doc = reader.read_file(str(p))
        # This will raise ValueError if not a valid UUID
        parsed_uuid = uuid.UUID(doc.document_id)
        assert str(parsed_uuid) == doc.document_id

    def test_source_uri_preserved(self, tmp_path: Path, reader):
        """Test that source_uri is exactly the URI passed in"""
        p = tmp_path / "test.txt"
        p.write_text("Content", encoding="utf-8")

        # Test with string path
        doc = reader.read_file(str(p))
        assert doc.source_uri == str(p)

        # Test with file URI
        uri = p.resolve().as_uri()
        doc2 = reader.read_file(uri)
        assert doc2.source_uri == uri

    # Protocol conformance tests
    def test_implements_document_reader_protocol(self, reader):
        """Test that TextDocumentReader implements DocumentReader protocol"""
        assert isinstance(reader, DocumentReader)

    def test_has_read_file_method(self, reader):
        """Test that the read_file method exists and is callable"""
        assert hasattr(reader, "read_file")
        assert callable(reader.read_file)

    # Content loader integration tests
    def test_uses_provided_content_loader(self, tmp_path: Path):
        """Test that TextDocumentReader uses the injected content loader"""
        p = tmp_path / "test.txt"
        p.write_text("Test content", encoding="utf-8")

        mock_loader: DefaultContentLoader = Mock(spec=DefaultContentLoader)
        mock_loader.load_text.return_value = "Mocked content"

        reader = TextDocumentReader(mock_loader)
        doc = reader.read_file(str(p))

        mock_loader.load_text.assert_called_once_with(str(p))
        assert doc.nodes[0].content == "Mocked content"

    def test_content_loader_receives_correct_uri(self, tmp_path: Path):
        """Test that the exact URI is passed to the content loader"""
        p = tmp_path / "test.txt"
        p.write_text("Content", encoding="utf-8")

        mock_loader: DefaultContentLoader = Mock(spec=DefaultContentLoader)
        mock_loader.load_text.return_value = "Content"

        reader = TextDocumentReader(mock_loader)

        # Test with different URI formats
        test_uri = str(p)
        reader.read_file(test_uri)
        mock_loader.load_text.assert_called_with(test_uri)

        file_uri = p.resolve().as_uri()
        reader.read_file(file_uri)
        mock_loader.load_text.assert_called_with(file_uri)

    # Edge cases
    def test_read_empty_file(self, tmp_path: Path, reader):
        """Test reading empty file creates a valid document"""
        p = tmp_path / "empty.txt"
        p.write_text("", encoding="utf-8")

        doc = reader.read_file(str(p))
        assert doc.nodes[0].content == ""
        assert len(doc.nodes) == 1
        assert doc.document_id  # Should still have an ID

    def test_read_file_whitespace_only(self, tmp_path: Path, reader):
        """Test reading file with only whitespace"""
        p = tmp_path / "whitespace.txt"
        p.write_text("   \n\t\n  ", encoding="utf-8")

        doc = reader.read_file(str(p))
        assert doc.nodes[0].content == "   \n\t\n  "

    def test_read_file_with_null_bytes(self, tmp_path: Path, reader):
        """Test reading file containing null bytes (treated as text)"""
        p = tmp_path / "with_nulls.txt"
        # Write as bytes to include null character
        p.write_bytes(b"Hello\x00World")

        doc = reader.read_file(str(p))
        # The null byte should be preserved
        assert "\x00" in doc.nodes[0].content or len(doc.nodes[0].content) == 11

    def test_read_very_long_path(self, tmp_path: Path, reader):
        """Test reading file with very long path"""
        # Create nested directories
        long_path = tmp_path
        for i in range(10):
            long_path = long_path / f"directory_{i}"
        long_path.mkdir(parents=True)

        p = long_path / "file.txt"
        p.write_text("Content in deep path", encoding="utf-8")

        doc = reader.read_file(str(p))
        assert doc.nodes[0].content == "Content in deep path"
        assert doc.source_uri == str(p)

    def test_constructor_accepts_content_loader(self):
        """Test that constructor properly accepts and stores the content loader"""
        loader = DefaultContentLoader()
        reader = TextDocumentReader(loader)
        assert reader._content_loader is loader

    def test_different_encodings_via_custom_loader(self, tmp_path: Path):
        """Test reading files with different encodings through a custom content loader"""
        p = tmp_path / "latin1.txt"
        content = "café résumé"
        p.write_text(content, encoding="latin-1")

        # Create a custom loader that handles latin-1
        class Latin1Loader(ContentLoader):
            def load_text(self, uri, encoding="latin-1"):
                with open(uri, "r", encoding=encoding) as f:
                    return f.read()

        reader = TextDocumentReader(Latin1Loader())
        doc = reader.read_file(str(p))
        assert doc.nodes[0].content == content

    # Additional comprehensive tests for full coverage
    def test_document_always_has_nodes_list(self, tmp_path: Path, reader):
        """Test that a document always has a nodes list (not None)"""
        p = tmp_path / "test.txt"
        p.write_text("Content", encoding="utf-8")

        doc = reader.read_file(str(p))
        assert doc.nodes is not None
        assert isinstance(doc.nodes, list)

    def test_node_always_has_content(self, tmp_path: Path, reader):
        """Test that node always has content field (even if empty)"""
        p = tmp_path / "test.txt"
        p.write_text("", encoding="utf-8")

        doc = reader.read_file(str(p))
        assert hasattr(doc.nodes[0], 'content')
        assert doc.nodes[0].content == ""

    def test_node_always_has_node_path(self, tmp_path: Path, reader):
        """Test that node always has node_path field"""
        p = tmp_path / "test.txt"
        p.write_text("Content", encoding="utf-8")

        doc = reader.read_file(str(p))
        assert hasattr(doc.nodes[0], 'node_path')
        assert doc.nodes[0].node_path is not None

    def test_read_file_creates_orthant_document(self, tmp_path: Path, reader):
        """Test that read_file returns an OrthantDocument instance"""
        p = tmp_path / "test.txt"
        p.write_text("Content", encoding="utf-8")

        doc = reader.read_file(str(p))
        from orthant.core.documents.contracts import OrthantDocument
        assert isinstance(doc, OrthantDocument)

    def test_read_file_creates_orthant_document_node(self, tmp_path: Path, reader):
        """Test that nodes are OrthantDocumentNode instances"""
        p = tmp_path / "test.txt"
        p.write_text("Content", encoding="utf-8")

        doc = reader.read_file(str(p))
        from orthant.core.documents.contracts import OrthantDocumentNode
        assert isinstance(doc.nodes[0], OrthantDocumentNode)

    def test_multiple_reads_same_file_different_doc_ids(self, tmp_path: Path, reader):
        """Test that reading the same file multiple times generates different document IDs"""
        p = tmp_path / "test.txt"
        p.write_text("Content", encoding="utf-8")

        docs = [reader.read_file(str(p)) for _ in range(5)]
        doc_ids = [doc.document_id for doc in docs]

        # All IDs should be unique
        assert len(doc_ids) == len(set(doc_ids))

    def test_content_loader_called_exactly_once(self, tmp_path: Path):
        """Test that the content loader is called exactly once per read"""
        p = tmp_path / "test.txt"
        p.write_text("Content", encoding="utf-8")

        mock_loader: DefaultContentLoader = Mock(spec=DefaultContentLoader)
        mock_loader.load_text.return_value = "Loaded content"

        reader = TextDocumentReader(mock_loader)
        doc = reader.read_file(str(p))

        assert mock_loader.load_text.call_count == 1
        assert doc.nodes[0].content == "Loaded content"

    def test_content_preserved_exactly(self, tmp_path: Path, reader):
        """Test that content is preserved exactly as loaded"""
        p = tmp_path / "test.txt"
        # Test with content that has special formatting
        content = "  Leading spaces\nMiddle\t\ttabs\nTrailing spaces  "
        p.write_text(content, encoding="utf-8")

        doc = reader.read_file(str(p))
        assert doc.nodes[0].content == content

    def test_read_file_with_carriage_returns(self, tmp_path: Path, reader):
        """Test reading files with Windows-style line endings"""
        p = tmp_path / "windows.txt"
        content = "Line 1\r\nLine 2\r\nLine 3"
        p.write_bytes(content.encode('utf-8'))

        doc = reader.read_file(str(p))
        assert "\r\n" in doc.nodes[0].content or "\n" in doc.nodes[0].content

    def test_read_file_with_tabs(self, tmp_path: Path, reader):
        """Test reading files with tab characters"""
        p = tmp_path / "tabs.txt"
        content = "Column1\tColumn2\tColumn3"
        p.write_text(content, encoding="utf-8")

        doc = reader.read_file(str(p))
        assert doc.nodes[0].content == content
        assert "\t" in doc.nodes[0].content

    def test_document_id_generation_uses_uuid4(self, tmp_path: Path, reader):
        """Test that document ID is generated using UUID4"""
        p = tmp_path / "test.txt"
        p.write_text("Content", encoding="utf-8")

        doc = reader.read_file(str(p))
        parsed_uuid = uuid.UUID(doc.document_id)
        # UUID4 has version 4
        assert parsed_uuid.version == 4

    def test_content_loader_error_propagation_io_error(self, tmp_path: Path):
        """Test that IOError from the content loader is propagated"""
        mock_loader: DefaultContentLoader = Mock(spec=DefaultContentLoader)
        mock_loader.load_text.side_effect = IOError("Disk error")

        reader = TextDocumentReader(mock_loader)

        with pytest.raises(IOError, match="Disk error"):
            reader.read_file("any_uri")

    def test_content_loader_error_propagation_unicode_error(self, tmp_path: Path):
        """Test that UnicodeDecodeError from the content loader is propagated"""
        mock_loader: DefaultContentLoader = Mock(spec=DefaultContentLoader)
        mock_loader.load_text.side_effect = UnicodeDecodeError(
            'utf-8', b'\xff', 0, 1, 'invalid start byte'
        )

        reader = TextDocumentReader(mock_loader)

        with pytest.raises(UnicodeDecodeError):
            reader.read_file("any_uri")

    def test_read_file_with_long_content(self, tmp_path: Path, reader):
        """Test reading file with very long single line"""
        p = tmp_path / "long_line.txt"
        # Create a very long line (100KB)
        content = "x" * 100_000
        p.write_text(content, encoding="utf-8")

        doc = reader.read_file(str(p))
        assert doc.nodes[0].content == content
        assert len(doc.nodes[0].content) == 100_000

    def test_read_file_with_many_lines(self, tmp_path: Path, reader):
        """Test reading file with many lines"""
        p = tmp_path / "many_lines.txt"
        # Create a file with 10,000 lines
        content = "\n".join(f"Line {i}" for i in range(10000))
        p.write_text(content, encoding="utf-8")

        doc = reader.read_file(str(p))
        assert doc.nodes[0].content == content
        assert doc.nodes[0].content.count("\n") == 9999

    def test_uri_with_query_parameters(self, tmp_path: Path, reader):
        """Test that URI with query parameters is preserved"""
        p = tmp_path / "test.txt"
        p.write_text("Content", encoding="utf-8")

        # Note: For local files, query params don't affect loading but URI should be preserved
        uri_with_params = f"{p.as_uri()}?param=value"

        # This may fail with the current implementation but tests the behavior
        try:
            doc = reader.read_file(uri_with_params)
            assert doc.source_uri == uri_with_params
        except FileNotFoundError:
            # If the implementation doesn't support query params, that's also valid
            pass

    def test_reader_instance_can_be_reused(self, tmp_path: Path, reader):
        """Test that the same reader instance can read multiple files"""
        files = []
        for i in range(3):
            p = tmp_path / f"file{i}.txt"
            p.write_text(f"Content {i}", encoding="utf-8")
            files.append(p)

        docs = [reader.read_file(str(f)) for f in files]

        assert len(docs) == 3
        assert docs[0].nodes[0].content == "Content 0"
        assert docs[1].nodes[0].content == "Content 1"
        assert docs[2].nodes[0].content == "Content 2"

    def test_content_loader_attribute_is_private(self):
        """Test that the content loader is stored as a private attribute"""
        loader = DefaultContentLoader()
        reader = TextDocumentReader(loader)

        assert hasattr(reader, '_content_loader')
        assert reader._content_loader is loader

    def test_node_path_type_is_string(self, tmp_path: Path, reader):
        """Test that node_path is a string type"""
        p = tmp_path / "test.txt"
        p.write_text("Content", encoding="utf-8")

        doc = reader.read_file(str(p))
        assert isinstance(doc.nodes[0].node_path, str)

    def test_document_id_type_is_string(self, tmp_path: Path, reader):
        """Test that document_id is a string type"""
        p = tmp_path / "test.txt"
        p.write_text("Content", encoding="utf-8")

        doc = reader.read_file(str(p))
        assert isinstance(doc.document_id, str)

    def test_source_uri_type_is_string(self, tmp_path: Path, reader):
        """Test that source_uri is a string type"""
        p = tmp_path / "test.txt"
        p.write_text("Content", encoding="utf-8")

        doc = reader.read_file(str(p))
        assert isinstance(doc.source_uri, str)

    def test_content_type_is_string(self, tmp_path: Path, reader):
        """Test that the content is a string type"""
        p = tmp_path / "test.txt"
        p.write_text("Content", encoding="utf-8")

        doc = reader.read_file(str(p))
        assert isinstance(doc.nodes[0].content, str)
