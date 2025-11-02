import pytest
import fsspec
import uuid
import zipfile
from pathlib import Path

from orthant.documents import DefaultContentLoader


TEXT = "Café 🚀 — hello"
BYTES = b"\x00\xff\x10hello\xfe"

@pytest.fixture
def loader():
    return DefaultContentLoader()


@pytest.mark.unit
class TestDefaultContentLoader:
    @pytest.mark.parametrize(
        ("uri", "expected"),
        [
            ("data:,Hello%2C%20World%21", "Hello, World!"),  # URL-encoded
            ("data:text/plain;base64,SGVsbG8sIHdvcmxkIQ==", "Hello, world!"),  # base64
        ],
    )
    def test_load_text_from_data_uri(self, uri, expected):
        loader = DefaultContentLoader()
        assert loader.load_text(uri) == expected

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("uri", "expected"),
        [
            ("data:,Hello%2C%20World%21", "Hello, World!"),
            ("data:text/plain;base64,SGVsbG8sIHdvcmxkIQ==", "Hello, world!"),
        ],
    )
    async def test_load_text_async_from_data_uri(self, uri, expected):
        loader = DefaultContentLoader()
        assert await loader.load_text_async(uri) == expected

    def test_load_text_local_path(self, tmp_path: Path, loader: DefaultContentLoader):
        p = tmp_path / "hello.txt"
        p.write_text(TEXT, encoding="utf-8")
        assert loader.load_text(str(p)) == TEXT

    def test_load_text_file_uri(self, tmp_path: Path, loader: DefaultContentLoader):
        p = tmp_path / "hello2.txt"
        p.write_text(TEXT, encoding="utf-8")
        assert loader.load_text(p.resolve().as_uri()) == TEXT

    def test_load_bytes_local_path(self, tmp_path: Path, loader: DefaultContentLoader):
        p = tmp_path / "data.bin"
        p.write_bytes(BYTES)
        assert loader.load_bytes(str(p)) == BYTES

    @pytest.mark.asyncio
    async def test_load_text_async_local_path(self, tmp_path: Path, loader: DefaultContentLoader):
        p = tmp_path / "hello_async.txt"
        p.write_text(TEXT, encoding="utf-8")
        assert await loader.load_text_async(str(p)) == TEXT

    @pytest.mark.asyncio
    async def test_load_bytes_async_local_path(self, tmp_path: Path, loader: DefaultContentLoader):
        p = tmp_path / "data_async.bin"
        p.write_bytes(BYTES)
        assert await loader.load_bytes_async(str(p)) == BYTES

    def test_load_text_memory_fs(self, loader: DefaultContentLoader):
        uri = f"memory://{uuid.uuid4()}/hello.txt"
        with fsspec.open(uri, "wt", encoding="utf-8") as f:
            f.write(TEXT)
        assert loader.load_text(uri) == TEXT

    @pytest.mark.asyncio
    async def test_load_text_async_memory_fs(self, loader: DefaultContentLoader):
        uri = f"memory://{uuid.uuid4()}/hello_async.txt"
        with fsspec.open(uri, "wt", encoding="utf-8") as f:
            f.write(TEXT)
        assert await loader.load_text_async(uri) == TEXT

    def test_load_bytes_memory_fs(self, loader: DefaultContentLoader):
        uri = f"memory://{uuid.uuid4()}/blob.bin"
        with fsspec.open(uri, "wb") as f:
            f.write(BYTES)
        assert loader.load_bytes(uri) == BYTES

    @pytest.mark.asyncio
    async def test_load_bytes_async_memory_fs(self, loader: DefaultContentLoader):
        uri = f"memory://{uuid.uuid4()}/blob_async.bin"
        with fsspec.open(uri, "wb") as f:
            f.write(BYTES)
        assert await loader.load_bytes_async(uri) == BYTES

    def test_load_text_from_zip(self, tmp_path: Path, loader):
        archive = tmp_path / "arc.zip"
        # Create a valid zip with the inner file
        with zipfile.ZipFile(archive, mode="w") as z:
            z.writestr("inner/hello.txt", TEXT)

        inner_uri = f"zip://inner/hello.txt::{archive}"
        assert loader.load_text(inner_uri) == TEXT

    @pytest.mark.asyncio
    async def test_load_text_async_from_zip(self, tmp_path: Path, loader):
        archive = tmp_path / "arc_async.zip"
        with zipfile.ZipFile(archive, mode="w") as z:
            z.writestr("inner/hello_async.txt", TEXT)

        inner_uri = f"zip://inner/hello_async.txt::{archive}"
        assert await loader.load_text_async(inner_uri) == TEXT

    def test_load_bytes_from_zip(self, tmp_path: Path, loader):
        archive = tmp_path / "arc_bytes.zip"
        # Build a valid zip containing inner/data.bin
        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as z:
            z.writestr("inner/data.bin", BYTES)

        inner_uri = f"zip://inner/data.bin::{archive.as_posix()}"
        assert loader.load_bytes(inner_uri) == BYTES

    @pytest.mark.asyncio
    async def test_load_bytes_async_from_zip(self, tmp_path: Path, loader):
        archive = tmp_path / "arc_bytes_async.zip"
        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as z:
            z.writestr("inner/data_async.bin", BYTES)

        inner_uri = f"zip://inner/data_async.bin::{archive.as_posix()}"
        assert await loader.load_bytes_async(inner_uri) == BYTES

    # Error handling tests
    def test_load_text_file_not_found(self, tmp_path: Path, loader):
        """Test that loading non-existent file raises appropriate error"""
        non_existent = tmp_path / "does_not_exist.txt"
        with pytest.raises(FileNotFoundError):
            loader.load_text(str(non_existent))

    def test_load_bytes_file_not_found(self, tmp_path: Path, loader):
        """Test that loading non-existent binary file raises appropriate error"""
        non_existent = tmp_path / "does_not_exist.bin"
        with pytest.raises(FileNotFoundError):
            loader.load_bytes(str(non_existent))

    @pytest.mark.asyncio
    async def test_load_text_async_file_not_found(self, tmp_path: Path, loader):
        """Test that async loading non-existent file raises appropriate error"""
        non_existent = tmp_path / "does_not_exist_async.txt"
        with pytest.raises(FileNotFoundError):
            await loader.load_text_async(str(non_existent))

    @pytest.mark.asyncio
    async def test_load_bytes_async_file_not_found(self, tmp_path: Path, loader):
        """Test that async loading non-existent binary file raises appropriate error"""
        non_existent = tmp_path / "does_not_exist_async.bin"
        with pytest.raises(FileNotFoundError):
            await loader.load_bytes_async(str(non_existent))

    # Encoding tests
    @pytest.mark.parametrize(
        ("encoding", "text_content"),
        [
            ("utf-8", "Hello UTF-8 世界 🌍"),
            ("utf-16", "Hello UTF-16 мир"),
            ("latin-1", "Hello Latin-1 café"),
            ("ascii", "Hello ASCII world"),
        ],
    )
    def test_load_text_with_different_encodings(self, tmp_path: Path, loader, encoding, text_content):
        """Test loading text files with different encodings"""
        p = tmp_path / f"text_{encoding}.txt"
        p.write_text(text_content, encoding=encoding)
        result = loader.load_text(str(p), encoding=encoding)
        assert result == text_content

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("encoding", "text_content"),
        [
            ("utf-8", "Async UTF-8 世界 🌍"),
            ("utf-16", "Async UTF-16 мир"),
            ("latin-1", "Async Latin-1 café"),
        ],
    )
    async def test_load_text_async_with_different_encodings(
        self, tmp_path: Path, loader, encoding, text_content
    ):
        """Test async loading text files with different encodings"""
        p = tmp_path / f"async_text_{encoding}.txt"
        p.write_text(text_content, encoding=encoding)
        result = await loader.load_text_async(str(p), encoding=encoding)
        assert result == text_content

    def test_load_text_wrong_encoding_handling(self, tmp_path: Path, loader):
        """Test behavior when file is read with wrong encoding"""
        p = tmp_path / "utf16_file.txt"
        text = "UTF-16 text: мир"
        p.write_text(text, encoding="utf-16")

        # Reading UTF-16 file as UTF-8 should raise an error or produce garbled text
        # The exact behavior depends on the content, but we're testing that it doesn't crash
        try:
            result = loader.load_text(str(p), encoding="utf-8")
            # If it doesn't raise, the result should be different from original
            assert result != text
        except UnicodeDecodeError:
            # This is also acceptable behavior
            pass

    # Edge case tests
    def test_load_empty_text_file(self, tmp_path: Path, loader):
        """Test loading an empty text file"""
        p = tmp_path / "empty.txt"
        p.write_text("", encoding="utf-8")
        result = loader.load_text(str(p))
        assert result == ""

    def test_load_empty_binary_file(self, tmp_path: Path, loader):
        """Test loading an empty binary file"""
        p = tmp_path / "empty.bin"
        p.write_bytes(b"")
        result = loader.load_bytes(str(p))
        assert result == b""

    @pytest.mark.asyncio
    async def test_load_empty_text_file_async(self, tmp_path: Path, loader):
        """Test async loading an empty text file"""
        p = tmp_path / "empty_async.txt"
        p.write_text("", encoding="utf-8")
        result = await loader.load_text_async(str(p))
        assert result == ""

    @pytest.mark.asyncio
    async def test_load_empty_binary_file_async(self, tmp_path: Path, loader):
        """Test async loading an empty binary file"""
        p = tmp_path / "empty_async.bin"
        p.write_bytes(b"")
        result = await loader.load_bytes_async(str(p))
        assert result == b""

    def test_load_large_text_file(self, tmp_path: Path, loader):
        """Test loading a large text file"""
        p = tmp_path / "large.txt"
        # Create a file with ~1MB of text
        large_text = "Line of text\n" * 100_000
        p.write_text(large_text, encoding="utf-8")
        result = loader.load_text(str(p))
        assert result == large_text
        assert len(result) > 1_000_000

    def test_load_large_binary_file(self, tmp_path: Path, loader):
        """Test loading a large binary file"""
        p = tmp_path / "large.bin"
        # Create a file with ~1MB of binary data
        large_bytes = bytes(range(256)) * 4096
        p.write_bytes(large_bytes)
        result = loader.load_bytes(str(p))
        assert result == large_bytes
        assert len(result) > 1_000_000

    def test_load_text_with_special_characters_in_path(self, tmp_path: Path, loader):
        """Test loading files with special characters in the path"""
        # Create a directory with special characters
        special_dir = tmp_path / "dir with spaces" / "sub-dir"
        special_dir.mkdir(parents=True)
        p = special_dir / "file with spaces.txt"
        p.write_text(TEXT, encoding="utf-8")
        result = loader.load_text(str(p))
        assert result == TEXT

    def test_load_text_multiline(self, tmp_path: Path, loader):
        """Test loading multiline text files"""
        p = tmp_path / "multiline.txt"
        multiline_text = "Line 1\nLine 2\nLine 3\n\nLine 5 (after blank)"
        p.write_text(multiline_text, encoding="utf-8")
        result = loader.load_text(str(p))
        assert result == multiline_text
        assert result.count("\n") == 4

    def test_load_text_with_bom(self, tmp_path: Path, loader):
        """Test loading text files with BOM (Byte Order Mark)"""
        p = tmp_path / "bom.txt"
        text_with_bom = "Text with BOM"
        p.write_text(text_with_bom, encoding="utf-8-sig")
        result = loader.load_text(str(p), encoding="utf-8-sig")
        assert result == text_with_bom

    def test_load_bytes_binary_data(self, tmp_path: Path, loader):
        """Test loading various binary data patterns"""
        p = tmp_path / "binary.bin"
        # Test with all possible byte values
        binary_data = bytes(range(256))
        p.write_bytes(binary_data)
        result = loader.load_bytes(str(p))
        assert result == binary_data
        assert len(result) == 256

    def test_all_methods_present(self, loader):
        """Test that all protocol methods are implemented"""
        assert hasattr(loader, "load_text")
        assert hasattr(loader, "load_bytes")
        assert hasattr(loader, "load_text_async")
        assert hasattr(loader, "load_bytes_async")
        assert callable(loader.load_text)
        assert callable(loader.load_bytes)
        assert callable(loader.load_text_async)
        assert callable(loader.load_bytes_async)

    def test_load_text_preserves_newline_types(self, tmp_path: Path, loader):
        """Test that different newline types are preserved"""
        p = tmp_path / "newlines.txt"
        # Write with explicit newlines
        text_unix = "Line1\nLine2\nLine3"
        p.write_text(text_unix, encoding="utf-8", newline="\n")
        result = loader.load_text(str(p))
        assert result == text_unix

    def test_load_bytes_exact_match(self, tmp_path: Path, loader):
        """Test that binary data is loaded with exact byte-for-byte accuracy"""
        p = tmp_path / "exact.bin"
        # Create specific binary pattern
        binary_pattern = b"\x00\x01\x02\xff\xfe\xfd\x80\x7f"
        p.write_bytes(binary_pattern)
        result = loader.load_bytes(str(p))
        assert result == binary_pattern
        assert len(result) == len(binary_pattern)
        for i, byte in enumerate(binary_pattern):
            assert result[i] == byte

    @pytest.mark.asyncio
    async def test_async_methods_return_same_as_sync(self, tmp_path: Path, loader):
        """Test that async methods return the same results as sync methods"""
        # Text file
        text_file = tmp_path / "sync_async_text.txt"
        text_file.write_text(TEXT, encoding="utf-8")

        sync_text = loader.load_text(str(text_file))
        async_text = await loader.load_text_async(str(text_file))
        assert sync_text == async_text

        # Binary file
        binary_file = tmp_path / "sync_async_bytes.bin"
        binary_file.write_bytes(BYTES)

        sync_bytes = loader.load_bytes(str(binary_file))
        async_bytes = await loader.load_bytes_async(str(binary_file))
        assert sync_bytes == async_bytes
