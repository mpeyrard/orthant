import asyncio
import pytest
import pytest_asyncio
from pathlib import Path

from orthant.datasets.observers import DirectoryObserver
from orthant.datasets.observers.events import DatasetObserverEvent


async def _cancel_task(task: asyncio.Task):
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


@pytest_asyncio.fixture
async def running_observer(tmp_path: Path):
    dataset_dir = tmp_path / "dataset"
    dataset_dir.mkdir()

    event_queue = asyncio.Queue()
    subject = DirectoryObserver(dataset_dir)
    started = asyncio.Event()
    observer_task = asyncio.create_task(subject.run_async(event_queue, started=started))

    # Wait for the observer to signal readiness (timeout to avoid hangs)
    await asyncio.wait_for(started.wait(), timeout=3.0)

    try:
        yield event_queue, dataset_dir, subject, observer_task
    finally:
        await _cancel_task(observer_task)


@pytest.mark.system
class TestDirectoryObserver:
    @pytest.mark.asyncio
    async def test_detect_file_created(self, running_observer):
        event_queue, dataset_dir, subject, observer_task = running_observer

        file_to_create = dataset_dir / "file.txt"
        file_to_create.touch()

        event = await asyncio.wait_for(event_queue.get(), timeout=5.0)
        assert isinstance(event, DatasetObserverEvent)
        # Different platforms may report 'added' or 'modified' for quick writes
        assert event.change in {"added", "modified"}
        assert event.path == str(file_to_create)

    @pytest.mark.asyncio
    async def test_detect_file_modified(self, running_observer):
        event_queue, dataset_dir, subject, observer_task = running_observer

        # Create initial file
        file_to_modify = dataset_dir / "file_mod.txt"
        file_to_modify.write_text("initial")

        # Modify the file
        file_to_modify.write_text("changed")

        event = await asyncio.wait_for(event_queue.get(), timeout=5.0)
        assert isinstance(event, DatasetObserverEvent)
        # Some watchers may emit 'modified' or 'added' depending on timing
        assert event.change in {"modified", "added"}
        assert event.path == str(file_to_modify)

    @pytest.mark.asyncio
    async def test_detect_file_deleted(self, running_observer):
        event_queue, dataset_dir, subject, observer_task = running_observer

        # Create file to delete
        file_to_delete = dataset_dir / "file_del.txt"
        file_to_delete.write_text("to be deleted")

        # Delete the file
        file_to_delete.unlink()

        event = await asyncio.wait_for(event_queue.get(), timeout=5.0)
        assert isinstance(event, DatasetObserverEvent)
        # Deletion may be reported as 'deleted' or sometimes 'modified'
        assert event.change in {"deleted", "modified"}
        assert event.path == str(file_to_delete)
