import datetime as dt
from lancedb.pydantic import LanceModel, Vector


def make_document_chunk_lance_schema(n_dims: int) -> type[LanceModel]:
    """Creates the data type to represent a document chunk in Lance."""
    embedding_type = Vector(n_dims)
    class DocumentChunkLanceSchema(LanceModel):
        source_uri: str
        node_path: str
        node_chunk_index: int
        modality: str
        created_at: dt.datetime
        content: str
        embedding: embedding_type
    return DocumentChunkLanceSchema
