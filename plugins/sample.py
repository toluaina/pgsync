from pgsync import plugin


class VillainPlugin(plugin.Plugin):
    """Example Villain plugin."""
    name = 'Villain'

    def transform(self, doc, **kwargs):
        """Demonstrates how to modify a document."""
        doc_id = kwargs['_id']
        doc_index = kwargs['_index']

        if doc_id == 'x':
            # do something...
            pass
        if doc_index == 'myindex':
            # do another thing...
            pass

        doc['villain'] = 'Ronan'
        return doc


class HeroPlugin(plugin.Plugin):
    """Example Hero plugin."""
    name = 'Hero'

    def transform(self, doc, **kwargs):
        """Demonstrates how to modify a document."""
        doc_id = kwargs['_id']
        doc_index = kwargs['_index']

        if doc_id == 'x':
            # do something...
            pass
        if doc_index == 'myindex':
            # do another thing...
            pass

        doc['hero'] = 'Doctor Strange'
        return doc
