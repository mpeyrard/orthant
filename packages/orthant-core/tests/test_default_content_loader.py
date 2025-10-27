import pytest
import fsspec
import uuid
import zipfile
from pathlib import Path

from orthant.core.documents import DefaultContentLoader


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
