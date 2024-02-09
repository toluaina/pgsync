from pgsync import plugin


class GrootPlugin(plugin.Plugin):
    """I am Groot plugin."""

    name: str = "Groot"

    def transform(self, doc: dict, **kwargs) -> dict:
        """Demonstrates how to modify a doc."""
        doc_id: str = kwargs["_id"]
        doc_index: str = kwargs["_index"]

        if doc_id == "x":
            # do something...
            pass
        if doc_index == "myindex":
            # do another thing...
            pass

        doc["character"] = "Groot"
        return doc
