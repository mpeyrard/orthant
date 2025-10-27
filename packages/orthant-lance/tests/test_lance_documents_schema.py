import datetime as dt
import pyarrow as pa
import pytest
import lancedb

from orthant.lance.documents import make_document_chunk_lance_schema


@pytest.mark.unit
class TestLanceDocumentsSchema:
    def test_insert_lance_document(self, tmp_path):
        # --- setup
        n_dims = 1024
        doc_schema = make_document_chunk_lance_schema(n_dims)

        db = lancedb.connect(str(tmp_path))
        tbl = db.create_table("documents", schema=doc_schema)

        now = dt.datetime(2025, 1, 1, 12, 0, 0)

        docs = [
            doc_schema(
                source_uri="file://handbook.md",
                node_path="page/1/para/1",
                modality="text",
                created_at=now,
                content="Deploy with Cloudflare via Workers and KV.",
                embedding=[0.1] * n_dims,
            ),
            doc_schema(
                source_uri="file://handbook.md",
                node_path="page/2/figure/1",
                modality="image",
                created_at=now,
                content="Architecture diagram: two clusters and an API gateway.",
                embedding=[0.2] * n_dims,
            ),
        ]

        # --- act
        tbl.add(docs)

        # --- assert: row count
        arr = tbl.to_arrow()
        assert arr.num_rows == 2

        # --- assert: embedding column is fixed-size float32 with correct dim
        sch = arr.schema
        emb_field = sch.field("embedding")
        assert pa.types.is_fixed_size_list(emb_field.type)
        assert emb_field.type.list_size == n_dims
        assert pa.types.is_float32(emb_field.type.value_type)

        # --- assert: created_at is a timestamp (unit may vary by platform)
        ts_field = sch.field("created_at")
        assert pa.types.is_timestamp(ts_field.type)

        # --- assert: search returns the nearest content for a matching vector
        hits = tbl.search([0.1] * n_dims).metric("cosine").limit(1).to_list()
        assert hits and "Deploy with Cloudflare" in hits[0]["content"]

        # --- assert: modalities preserved
        contents = {row["node_path"]: row["modality"] for row in tbl.to_arrow().to_pylist()}
        assert contents["page/1/para/1"] == "text"
        assert contents["page/2/figure/1"] == "image"
