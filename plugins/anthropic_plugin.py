"""Plugin for generating embeddings using Anthropic's Voyage AI."""

import typing as t
from functools import lru_cache

from pgsync import plugin


class AnthropicPlugin(plugin.Plugin):
    """
    Generate embeddings using Anthropic's Voyage AI embeddings.

    Requires: `pip install voyageai`

    Set VOYAGE_API_KEY environment variable for authentication.
    Uses the voyage-3 model which produces 1024-dimensional vectors
    optimized for retrieval and semantic similarity.

    See: https://docs.anthropic.com/en/docs/build-with-claude/embeddings
    """

    name: str = "Anthropic"

    # voyage-3 produces 1024-dimensional vectors
    MODEL: str = "voyage-3"
    VECTOR_DIMS: int = 1024

    def __init__(self) -> None:
        super().__init__()
        import voyageai

        self.client = voyageai.Client()

    @lru_cache(maxsize=1024)
    def get_embedding(self, text: str) -> t.Tuple[float, ...]:
        """Generate embedding for text, with caching."""
        text = text.replace("\n", " ")
        result = self.client.embed(
            texts=[text],
            model=self.MODEL,
            input_type="document",
        )
        return tuple(result.embeddings[0])

    def transform(self, doc: dict, **kwargs) -> dict:
        """Add Voyage AI embedding to document."""
        # Customize this to use your document's text field(s)
        text = doc.get("title", "") or doc.get("description", "")
        if not text:
            return doc

        embedding = self.get_embedding(text)
        doc["embedding"] = list(embedding)
        return doc
