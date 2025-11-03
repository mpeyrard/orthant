import pytest
from orthant.datasets import InMemoryDatasetRegistry, DatasetSpec


@pytest.mark.unit
class TestInMemoryDatasetRegistry:
    def test_insert(self):
        registry = InMemoryDatasetRegistry()
        spec = DatasetSpec(dataset_id="test", storage_format="test", query_format="default", uri="test")
        registry.insert(spec)
        assert registry.get_dataset("test") is spec

    def test_duplicate_insert_not_allowed(self):
        registry = InMemoryDatasetRegistry()
        registry.insert(DatasetSpec(dataset_id="test", storage_format="test", query_format="default", uri="test"))
        with pytest.raises(ValueError):
            registry.insert(DatasetSpec(dataset_id="test", storage_format="test", query_format="default", uri="test"))

    def test_get_dataset(self):
        registry = InMemoryDatasetRegistry()
        d1 = DatasetSpec(dataset_id="test1", storage_format="test", query_format="default", uri="test")
        d2 = DatasetSpec(dataset_id="test2", storage_format="test2", query_format="default", uri="test2")
        registry.insert(d1)
        registry.insert(d2)
        assert registry.get_dataset("test1") is d1
        assert registry.get_dataset("test2") is d2

    def test_list_datasets(self):
        registry = InMemoryDatasetRegistry()
        d1 = DatasetSpec(dataset_id="test1", storage_format="test", query_format="default", uri="test")
        d2 = DatasetSpec(dataset_id="test2", storage_format="test2", query_format="default", uri="test2")
        registry.insert(d1)
        registry.insert(d2)
        assert registry.list_datasets() == [d1, d2]
