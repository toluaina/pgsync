"""Example of organizing plugins in a nested package structure."""

from pgsync import plugin


class GrootPlugin(plugin.Plugin):
    """
    Example plugin in a nested package.

    Demonstrates how to organize plugins in subdirectories.
    Place plugins in `plugins/character/` and they will be
    automatically discovered by PGSync.
    """

    name: str = "Groot"

    def transform(self, doc: dict, **kwargs) -> dict:
        """Add a character field to the document."""
        doc["character"] = "Groot"
        return doc
