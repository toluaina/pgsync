from pgsync import plugin


class GrootPlugin(plugin.Plugin):
    """I am Groot plugin."""

    name = "Groot"

    def transform(self, doc, **kwargs):
        """Demonstrates how to modify a document."""
        doc_id = kwargs["_id"]
        doc_index = kwargs["_index"]

        if doc_id == "x":
            # do something...
            pass
        if doc_index == "myindex":
            # do another thing...
            pass

        doc["character"] = "Groot"
        return doc
