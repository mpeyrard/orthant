"""Haystack integration helpers for Orthant.

This package contains utilities that adapt Haystack components (e.g. the
DocumentPreprocessor) to Orthant's protocols such as `ChunkingStrategy`.
"""

from .chunking import HaystackChunkingStrategy

# Keep explicit __all__ out of package __init__ to simplify imports and avoid
# requiring maintenance when adding new utilities.
