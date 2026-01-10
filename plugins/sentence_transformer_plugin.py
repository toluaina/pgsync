"""Plugin for generating embeddings using Sentence Transformers."""

import typing as t

from pgsync import plugin


class SentenceTransformersPlugin(plugin.Plugin):
    """
    Generate embeddings using Sentence Transformers.

    Requires: `pip install sentence-transformers`

    Uses the all-MiniLM-L6-v2 model which produces 384-dimensional vectors.
    This is a lightweight model suitable for semantic search applications.

    See: https://www.sbert.net/docs/pretrained_models.html
    """

    name: str = "SentenceTransformer"

    # all-MiniLM-L6-v2 produces 384-dimensional vectors
    MODEL_NAME: str = "all-MiniLM-L6-v2"
    VECTOR_DIMS: int = 384

    def __init__(self) -> None:
        super().__init__()
        from sentence_transformers import SentenceTransformer

        self.model: SentenceTransformer = SentenceTransformer(self.MODEL_NAME)

    def get_embedding(self, text: str) -> t.List[float]:
        """Generate embedding for text."""
        text = text.replace("\n", " ")
        return self.model.encode(text).tolist()

    def transform(self, doc: dict, **kwargs) -> dict:
        """Add Sentence Transformer embedding to document."""
        # Customize this to use your document's text field(s)
        text = doc.get("title", "") or doc.get("description", "")
        if not text:
            return doc

        embedding = self.get_embedding(text)
        doc["embedding"] = embedding
        return doc
