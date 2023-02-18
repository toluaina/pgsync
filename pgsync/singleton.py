"""PGSync Singleton."""

from typing import Tuple


class Singleton(type):
    _instances: dict = {}

    def __call__(cls, *args, **kwargs):
        if not args:
            return super(Singleton, cls).__call__(*args, **kwargs)
        database: str = args[0]["database"]
        index: str = args[0].get("index", database)
        key: Tuple[str, str] = (database, index)
        if key not in cls._instances:
            cls._instances[key] = super(Singleton, cls).__call__(
                *args, **kwargs
            )
        return cls._instances[key]
