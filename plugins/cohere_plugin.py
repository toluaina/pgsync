"""Plugin for generating embeddings using Cohere."""

import typing as t
from functools import lru_cache

from pgsync import plugin


class CoherePlugin(plugin.Plugin):
    """
    Generate embeddings using Cohere's embed-english-v3.0 model.

    Requires: `pip install cohere`

    Set COHERE_API_KEY environment variable for authentication.
    Uses the embed-english-v3.0 model which produces 1024-dimensional vectors
    optimized for semantic search and retrieval.

    See: https://docs.cohere.com/docs/embeddings
    """

    name: str = "Cohere"

    # embed-english-v3.0 produces 1024-dimensional vectors
    MODEL: str = "embed-english-v3.0"
    VECTOR_DIMS: int = 1024

    def __init__(self) -> None:
        super().__init__()
        import cohere

        self.client = cohere.Client()

    @lru_cache(maxsize=1024)
    def get_embedding(self, text: str) -> t.Tuple[float, ...]:
        """Generate embedding for text, with caching."""
        text = text.replace("\n", " ")
        response = self.client.embed(
            texts=[text],
            model=self.MODEL,
            input_type="search_document",
        )
        return tuple(response.embeddings[0])

    def transform(self, doc: dict, **kwargs) -> dict:
        """Add Cohere embedding to document."""
        # Customize this to use your document's text field(s)
        text = doc.get("title", "") or doc.get("description", "")
        if not text:
            return doc

        embedding = self.get_embedding(text)
        doc["embedding"] = list(embedding)
        return doc
