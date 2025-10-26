import os
from mistralai import Mistral


api_key = os.getenv("MISTRAL_API_KEY")
mistral = Mistral(api_key=api_key)



class MistralEmbeddingModel:
    def __init__(self, mistral_client: Mistral = mistral):
        self._client = mistral_client

    def encode(self, text: str) -> list[float]:
        embedding = self._client.embeddings.create(model="mistral-embed", inputs=[text])
        return embedding.data[0].embedding

    async def encode_async(self, text: str) -> list[float]:
        embedding = await self._client.embeddings.create_async(model="mistral-embed", inputs=[text])
        return embedding.data[0].embedding

    def encode_batch(self, batch: list[str]) -> list[list[float]]:
        embeddings = self._client.embeddings.create(model="mistral-embed", inputs=batch)
        return list([e.embedding for e in embeddings.data])

    async def encode_batch_async(self, batch: list[str]) -> list[list[float]]:
        embeddings = await self._client.embeddings.create_async(model="mistral-embed", inputs=batch)
        return list([e.embedding for e in embeddings.data])
