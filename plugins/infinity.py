"""Plugin for handling PostgreSQL infinity date values."""

import datetime

from pgsync import plugin


class InfinityPlugin(plugin.Plugin):
    """
    Convert PostgreSQL infinity date values to Python datetime.

    PostgreSQL supports 'infinity' and '-infinity' as special date values.
    This plugin converts them to datetime.max and datetime.min respectively,
    making them compatible with Elasticsearch date fields.
    """

    name: str = "Infinity"

    def transform(self, doc: dict, **kwargs) -> dict:
        """Convert infinity date strings to datetime objects."""
        publish_date = doc.get("publish_date")
        if isinstance(publish_date, str):
            if publish_date.lower() == "infinity":
                doc["publish_date"] = datetime.datetime.max
            elif publish_date.lower() == "-infinity":
                doc["publish_date"] = datetime.datetime.min
        return doc
