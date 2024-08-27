from pgsync import plugin


class SentenceTransformersPlugin(plugin.Plugin):
    """
    I a sentence transformer plugin.
    I generate embeddings for documents using SentenceTransformers's all-MiniLM-L6-v2 model.
    `pip install sentence-transformers`

    https://www.elastic.co/search-labs/tutorials/search-tutorial/vector-search/generate-embeddings
    """

    def __init__(self) -> None:
        super().__init__()
        from sentence_transformers import SentenceTransformer

        self.model: SentenceTransformer = SentenceTransformer(
            "all-MiniLM-L6-v2"
        )
        # vector dims must match models input dims
        self.vector_dims: int = 1536

    name: str = "SentenceTransformer"

    def get_embedding(self, text: str) -> list:
        text: str = text.replace("\n", " ")
        return self.model.encode(text)

    def transform(self, doc: dict, **kwargs) -> dict:
        """Demonstrates how to generate SentenceTransformers embeddings and add them to the document"""
        fields = doc["book_title"]
        embedding: list = self.get_embedding(fields)
        if len(embedding) != self.vector_dims:
            raise ValueError(f"Embedding dims != {self.vector_dims}.")

        doc["embedding"] = embedding
        return doc
