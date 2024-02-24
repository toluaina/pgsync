from functools import lru_cache

from openai import OpenAI

from pgsync import plugin


class OpenAIPlugin(plugin.Plugin):
    """
    I am an OpenAI plugin.
    I generate embeddings for documents using openai's text-embedding-3-small model.
    `pip install openai`
    """

    def __init__(self) -> None:
        super().__init__()
        self.client: OpenAI = OpenAI()
        self.model: str = "text-embedding-3-small"
        # vector dims must match models input dims
        self.vector_dims = 1536

    name: str = "TextEmbedding3Small"

    @lru_cache
    def get_embedding(self, text: str) -> list:
        text: str = text.replace("\n", " ")
        return (
            self.client.embeddings.create(input=[text], model=self.model)
            .data[0]
            .embedding
        )

    def transform(self, doc: dict, **kwargs) -> dict:
        """Demonstrates how to generate openai embeddings and add them to the document"""
        fields = doc["book_title"]
        embedding: list = self.get_embedding(fields)
        if len(embedding) != self.vector_dims:
            raise ValueError(f"Embedding dims != {self.vector_dims}.")

        doc["embedding"] = embedding
        return doc
