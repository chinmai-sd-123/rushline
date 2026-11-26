"""Text embeddings for policy search: OpenAI when configured, a hashing fake otherwise.

The fake is deterministic and lexical (words and word pairs hashed into the vector), so
tests and offline demos get sensible similarity without a network call. Query and stored
vectors must come from the same embedder: re-run ingestion after switching.
"""

import hashlib
import math
import re
from typing import Protocol

DIMENSIONS = 256  # matches policy_chunks.embedding vector(256)
_WORD = re.compile(r"[a-z0-9]+")


class Embedder(Protocol):
    name: str

    async def embed(self, texts: list[str]) -> list[list[float]]: ...


class HashEmbedder:
    name = "hash-256"

    async def embed(self, texts: list[str]) -> list[list[float]]:
        return [self._one(t) for t in texts]

    @staticmethod
    def _one(text: str) -> list[float]:
        words = _WORD.findall(text.lower())
        vector = [0.0] * DIMENSIONS
        for feature in [*words, *(f"{a} {b}" for a, b in zip(words, words[1:], strict=False))]:
            digest = hashlib.blake2b(feature.encode(), digest_size=8).digest()
            index = int.from_bytes(digest[:4], "little") % DIMENSIONS
            vector[index] += 1.0 if digest[4] & 1 else -1.0  # signed hashing cancels collisions
        norm = math.sqrt(sum(v * v for v in vector)) or 1.0
        return [v / norm for v in vector]


class OpenAIEmbedder:
    def __init__(self, api_key: str, model: str):
        from openai import AsyncOpenAI

        self.client = AsyncOpenAI(api_key=api_key, max_retries=2)
        self.model = model
        self.name = f"{model}-{DIMENSIONS}"

    async def embed(self, texts: list[str]) -> list[list[float]]:
        response = await self.client.embeddings.create(
            model=self.model, input=texts, dimensions=DIMENSIONS
        )
        return [item.embedding for item in response.data]


def make_embedder(settings) -> Embedder:
    if settings.openai_api_key is not None:
        return OpenAIEmbedder(settings.openai_api_key.get_secret_value(), settings.embedding_model)
    return HashEmbedder()


def to_pgvector(vector: list[float]) -> str:
    return "[" + ",".join(f"{v:.6f}" for v in vector) + "]"
