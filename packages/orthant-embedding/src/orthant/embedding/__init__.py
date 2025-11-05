"""Embedding helpers and protocols for Orthant.

This package contains the embedding-related interfaces used by Orthant, such
as chunking strategies and embedding client protocols.
"""

from .chunking_strategy import ChunkingStrategy
from .document_contracts import VectorDocumentNodeChunk
from .vector_document_storage import VectorDocumentStorage
from .vector_ingest_pipeline import VectorIngestPipeline
