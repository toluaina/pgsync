"""Sample plugins demonstrating common PGSync plugin patterns."""

import typing as t

from pgsync import plugin


class AuthPlugin(plugin.Plugin):
    """
    Example authentication plugin.

    Provides credentials for database, Elasticsearch, and Redis connections.
    Implement the `auth` method to return credentials from a secrets manager,
    environment variables, or other secure storage.
    """

    name: str = "Auth"

    def transform(self, doc: dict, **kwargs) -> dict:
        return doc

    def auth(self, key: str) -> t.Optional[str]:
        """
        Return credentials for the given key.

        Args:
            key: One of 'PG_PASSWORD', 'ELASTICSEARCH_PASSWORD', or 'REDIS_AUTH'

        Returns:
            The credential string, or None to use default configuration.
        """
        credentials = {
            "PG_PASSWORD": None,
            "ELASTICSEARCH_PASSWORD": None,
            "REDIS_AUTH": None,
        }
        return credentials.get(key)


class VillainPlugin(plugin.Plugin):
    """
    Example plugin that adds a static field to documents.

    Demonstrates basic document modification with conditional logic
    based on document ID or index name.
    """

    name: str = "Villain"

    def transform(self, doc: dict, **kwargs) -> dict:
        """Add a villain field to the document."""
        doc["villain"] = "Ronan"
        return doc


class HeroPlugin(plugin.Plugin):
    """
    Example plugin that adds a static field to documents.

    Demonstrates the basic plugin pattern for enriching documents
    with additional data during sync.
    """

    name: str = "Hero"

    def transform(self, doc: dict, **kwargs) -> dict:
        """Add a hero field to the document."""
        doc["hero"] = "Doctor Strange"
        return doc


class GeometryPlugin(plugin.Plugin):
    """
    Plugin for transforming PostGIS geometry fields.

    Converts GeoJSON Point and Polygon types to Elasticsearch-compatible
    geo_point and geo_shape formats.
    """

    name: str = "Geometry"

    def transform(self, doc: dict, **kwargs) -> dict:
        """Transform geometry fields to Elasticsearch geo types."""
        if not doc:
            return doc

        # Convert GeoJSON Point to geo_point coordinates
        point = doc.get("point")
        if point and point.get("type") == "Point":
            doc["coordinates"] = point["coordinates"]

        # Convert GeoJSON Polygon to geo_shape
        polygon = doc.get("polygon")
        if polygon and polygon.get("type") == "Polygon":
            doc["shape"] = polygon

        return doc
