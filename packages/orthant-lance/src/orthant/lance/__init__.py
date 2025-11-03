"""LanceDB integration for Orthant.

This package provides helpers and a storage implementation for persisting
Orthant documents into LanceDB tables.
"""

from .documents import make_document_chunk_lance_schema
from .document_storage import LanceDocumentStorage
