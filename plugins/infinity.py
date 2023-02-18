import datetime

from pgsync import plugin


class InfinityPlugin(plugin.Plugin):
    name: str = "Infinity"

    def transform(self, doc: dict, **kwargs) -> dict:
        """Demonstrates infinity transform."""
        doc_index: str = kwargs["_index"]
        if doc_index == "book":
            if doc.get("publish_date"):
                if doc["publish_date"].lower() == "infinity":
                    doc["publish_date"] = datetime.datetime.max
                elif doc["publish_date"].lower() == "-infinity":
                    doc["publish_date"] = datetime.datetime.min
        return doc
