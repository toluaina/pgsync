from pgsync import plugin


class VillainPlugin(plugin.Plugin):
    """Example Villain plugin."""
    name = 'Villain'

    def transform(self, doc):
        """Demonstrates how to modify a document."""
        doc['villain'] = 'Ronan'
        return doc


class HeroPlugin(plugin.Plugin):
    """Example Hero plugin."""
    name = 'Hero'

    def transform(self, doc):
        """Demonstrates how to modify a document."""
        doc['hero'] = 'Doctor Strange'
        return doc
