from functools import lru_cache

from plugins.openai_plugin import OpenAIPlugin


@lru_cache
def get_embedding(text: str) -> list:
    plugin: OpenAIPlugin = OpenAIPlugin()
    return plugin.get_embedding(text)
