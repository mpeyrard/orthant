# Document Ingestion Pipeline - Usage Guide

## Overview

The `DocumentIngestionPipeline` combines three components to transform documents into embedded vectors ready for storage:

1. **DocumentReader** - Loads documents from URIs
2. **ChunkingStrategy** - Splits documents into manageable chunks
3. **EmbeddingModel** - Generates vector embeddings from text

## Basic Usage

```python
from orthant.core import DocumentIngestionPipeline
from orthant.core.documents import TextDocumentReader, DefaultContentLoader, DocumentReaderDispatcher
from orthant.haystack import HaystackChunkingStrategy
from orthant.mistral import MistralEmbeddingModel

# Create components
content_loader = DefaultContentLoader()
reader = TextDocumentReader(content_loader)
chunker = HaystackChunkingStrategy.sentence_splitter(n_sentences=6, overlap=2)
embedder = MistralEmbeddingModel()

# Create pipeline
pipeline = DocumentIngestionPipeline(
    reader=reader,
    chunker=chunker,
    embedder=embedder,
    modality="text"
)

# Ingest a single document
chunks = pipeline.ingest("path/to/document.txt")

# Each chunk has:
# - source_uri: original document URI
# - node_path: location in document structure
# - node_chunk_index: chunk number within node
# - content: text content
# - embedding: vector representation
# - modality: content type
# - created_at: timestamp
```

## Advanced Usage

### Using the Dispatcher for Multiple File Types

```python
from orthant.core.documents import DocumentReaderDispatcher

# Create specialized readers
text_reader = TextDocumentReader(content_loader)
# pdf_reader = PDFDocumentReader(content_loader)  # Future
# html_reader = HTMLDocumentReader(content_loader)  # Future

# Create dispatcher
dispatcher = DocumentReaderDispatcher([text_reader])
# dispatcher.register_reader(pdf_reader, priority=10)
# dispatcher.register_reader(html_reader, priority=5)

# Use dispatcher in pipeline
pipeline = DocumentIngestionPipeline(
    reader=dispatcher,
    chunker=chunker,
    embedder=embedder,
)

# Now can handle multiple file types
chunks = pipeline.ingest("document.txt")  # Uses TextDocumentReader
# chunks = pipeline.ingest("report.pdf")   # Would use PDFDocumentReader
```

### Async Ingestion

```python
import asyncio

async def ingest_documents():
    chunks = await pipeline.ingest_async("large_document.txt")
    return chunks

# Run async
chunks = asyncio.run(ingest_documents())
```

### Batch Ingestion

```python
file_uris = [
    "doc1.txt",
    "doc2.txt",
    "doc3.txt",
]

# Sync batch
all_chunks = pipeline.ingest_batch(file_uris)

# Async batch
all_chunks = await pipeline.ingest_batch_async(file_uris)
```

### Custom Modality

```python
pipeline = DocumentIngestionPipeline(
    reader=markdown_reader,
    chunker=chunker,
    embedder=embedder,
    modality="markdown",  # Custom modality tag
)
```

## Storing Results in Lance

```python
from orthant.lance.documents import make_document_chunk_lance_schema
import lancedb

# Create Lance schema (embedding dimension must match your model)
DocumentChunkSchema = make_document_chunk_lance_schema(n_dims=1024)

# Ingest documents
chunks = pipeline.ingest("document.txt")

# Convert to Lance format
lance_chunks = [
    DocumentChunkSchema(
        source_uri=chunk.source_uri,
        node_path=chunk.node_path,
        node_chunk_index=chunk.node_chunk_index,
        modality=chunk.modality,
        created_at=chunk.created_at,
        content=chunk.content,
        embedding=chunk.embedding,
    )
    for chunk in chunks
]

# Store in LanceDB
db = lancedb.connect("my_database")
table = db.create_table("documents", data=lance_chunks)
```

## Complete Example

```python
from orthant.core import DocumentIngestionPipeline
from orthant.core.documents import TextDocumentReader, DefaultContentLoader
from orthant.haystack import HaystackChunkingStrategy
from orthant.mistral import MistralEmbeddingModel
from orthant.lance.documents import make_document_chunk_lance_schema
import lancedb

# 1. Set up components
content_loader = DefaultContentLoader()
reader = TextDocumentReader(content_loader)
chunker = HaystackChunkingStrategy.sentence_splitter(n_sentences=6, overlap=2)
embedder = MistralEmbeddingModel()

# 2. Create pipeline
pipeline = DocumentIngestionPipeline(
    reader=reader,
    chunker=chunker,
    embedder=embedder,
)

# 3. Ingest documents
document_uris = [
    "data/doc1.txt",
    "data/doc2.txt",
    "data/doc3.txt",
]

all_chunks = pipeline.ingest_batch(document_uris)

# 4. Store in Lance
DocumentChunkSchema = make_document_chunk_lance_schema(n_dims=1024)

lance_chunks = [
    DocumentChunkSchema(
        source_uri=chunk.source_uri,
        node_path=chunk.node_path,
        node_chunk_index=chunk.node_chunk_index,
        modality=chunk.modality,
        created_at=chunk.created_at,
        content=chunk.content,
        embedding=chunk.embedding,
    )
    for chunk in all_chunks
]

db = lancedb.connect("vector_db")
table = db.create_table("document_chunks", data=lance_chunks)

print(f"Ingested {len(all_chunks)} chunks from {len(document_uris)} documents")
```

## EmbeddedDocumentChunk Structure

```python
@dataclass
class EmbeddedDocumentChunk:
    source_uri: str              # Document identifier
    node_path: str               # Location in document structure
    node_chunk_index: int        # Chunk number within node
    content: str                 # Text content
    embedding: list[float]       # Vector representation
    modality: str = "text"       # Content type (text, markdown, etc.)
    created_at: dt.datetime | None = None  # Ingestion timestamp
```

## Error Handling

```python
try:
    chunks = pipeline.ingest("nonexistent.txt")
except FileNotFoundError:
    print("Document not found")

try:
    chunks = pipeline.ingest("unsupported.xyz")
except ValueError as e:
    print(f"No reader available: {e}")
```

## Performance Tips

1. **Batch Processing**: Use `ingest_batch()` for multiple documents to amortize overhead
2. **Async for I/O**: Use `ingest_async()` for better concurrency with remote documents
3. **Batch Embeddings**: The pipeline automatically batches all chunks from a document for efficient embedding
4. **Chunk Size**: Tune `n_sentences` in the chunker based on your embedding model's context window

## Integration with Existing Systems

The pipeline output is a simple list of `EmbeddedDocumentChunk` dataclasses, making it easy to integrate with any vector storage system:

```python
# Convert to your format
for chunk in chunks:
    your_system.add_vector(
        id=f"{chunk.source_uri}#{chunk.node_path}#{chunk.node_chunk_index}",
        vector=chunk.embedding,
        metadata={
            "content": chunk.content,
            "modality": chunk.modality,
            "created_at": chunk.created_at,
        }
    )
```
