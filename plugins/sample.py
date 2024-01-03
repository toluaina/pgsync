import typing as t

from pgsync import plugin


class Auth(plugin.Plugin):
    """Example auth plugin."""

    name: str = "Auth"

    def transform(self, doc: dict, **kwargs) -> dict:
        pass

    def auth(self, key: str) -> t.Optional[str]:
        """Sample auth."""
        if key == "PG_PASSWORD":
            return None
        if key == "ELASTICSEARCH_PASSWORD":
            return None
        if key == "REDIS_AUTH":
            return None


class VillainPlugin(plugin.Plugin):
    """Example Villain plugin."""

    name: str = "Villain"

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

        doc["villain"] = "Ronan"
        return doc


class HeroPlugin(plugin.Plugin):
    """Example Hero plugin."""

    name: str = "Hero"

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

        doc["hero"] = "Doctor Strange"
        return doc


class GeometryPlugin(plugin.Plugin):
    """Example plugin demonstrating GeoPoint and GeoShape."""

    name: str = "Geometry"

    def transform(self, doc: dict, **kwargs) -> dict:
        """Demonstrates how to modify a doc."""
        doc_index: str = kwargs["_index"]

        if doc_index == "book":
            if doc and doc.get("point"):
                if doc["point"]["type"] == "Point":
                    doc["coordinates"] = doc["point"]["coordinates"]

            if doc and doc.get("polygon"):
                if doc["polygon"]["type"] == "Polygon":
                    doc["shape"] = doc["polygon"]

        return doc
