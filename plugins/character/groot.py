from pgsync import plugin


class GrootPlugin(plugin.Plugin):
    """I am Groot plugin."""
    name = 'Groot'

    def transform(self, doc):
        """Demonstrates how to modify a document."""
        doc['character'] = 'Groot'
        return doc
