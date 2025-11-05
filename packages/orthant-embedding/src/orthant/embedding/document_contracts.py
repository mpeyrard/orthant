from typing import Sequence
from orthant.documents import OrthantDocumentNodeChunk


class VectorDocumentNodeChunk(OrthantDocumentNodeChunk):
    """Represents a vector node chunk."""
    vector: Sequence[float]
