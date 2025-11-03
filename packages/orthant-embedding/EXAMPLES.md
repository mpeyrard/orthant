Examples for orthant-embedding

This file contains minimal examples for implementing a ChunkingStrategy and
using EmbeddingClient implementations.

Example ChunkingStrategy (trivial):

```python
from orthant.embedding import ChunkingStrategy
from orthant.documents import OrthantDocument, OrthantDocumentNodeChunk

class SimpleChunker(ChunkingStrategy):
    def chunk_document(self, document: OrthantDocument) -> list[OrthantDocumentNodeChunk]:
        chunks = []
        for node in document.nodes:
            chunks.append(OrthantDocumentNodeChunk(document_id=document.document_id, node_path=node.node_path, node_chunk_index=0, content=node.content))
        return chunks
```

Using an EmbeddingClient (Mistral example):

```python
from orthant.mistral import MistralEmbeddingClient

client = MistralEmbeddingClient(api_key="...")
embedding = await client.encode_document_async("Hello world")
```
