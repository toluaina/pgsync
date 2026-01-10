"""Plugin for generating OpenAI embeddings."""

import typing as t
from functools import lru_cache

from pgsync import plugin


class OpenAIPlugin(plugin.Plugin):
    """
    Generate embeddings using OpenAI's text-embedding-3-small model.

    Requires: `pip install openai`

    Set OPENAI_API_KEY environment variable for authentication.
    The embedding is added to the document as an 'embedding' field,
    suitable for Elasticsearch dense_vector or OpenSearch knn_vector fields.
    """

    name: str = "OpenAI"

    # text-embedding-3-small produces 1536-dimensional vectors
    MODEL: str = "text-embedding-3-small"
    VECTOR_DIMS: int = 1536

    def __init__(self) -> None:
        super().__init__()
        from openai import OpenAI

        self.client: OpenAI = OpenAI()

    @lru_cache(maxsize=1024)
    def get_embedding(self, text: str) -> t.Tuple[float, ...]:
        """Generate embedding for text, with caching."""
        text = text.replace("\n", " ")
        response = self.client.embeddings.create(
            input=[text],
            model=self.MODEL,
        )
        return tuple(response.data[0].embedding)

    def transform(self, doc: dict, **kwargs) -> dict:
        """Add OpenAI embedding to document."""
        # Customize this to use your document's text field(s)
        text = doc.get("title", "") or doc.get("description", "")
        if not text:
            return doc

        embedding = self.get_embedding(text)
        doc["embedding"] = list(embedding)
        return doc
