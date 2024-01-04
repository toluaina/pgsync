"""PGSync Singleton."""

import typing as t


class Singleton(type):
    """
    A metaclass that allows a class to have only one instance.

    Usage:
    class MyClass(metaclass=Singleton):
        pass
    """

    _instances: dict = {}

    def __call__(cls, *args, **kwargs):
        """
        If an instance of the class has already been created with the same arguments,
        return that instance. Otherwise, create a new instance and return it.

        Args:
        cls: The class object.
        *args: Positional arguments to be passed to the class constructor.
        **kwargs: Keyword arguments to be passed to the class constructor.

        Returns:
        An instance of the class.
        """
        if not args:
            return super(Singleton, cls).__call__(*args, **kwargs)
        database: str = args[0]["database"]
        index: str = args[0].get("index", database)
        key: t.Tuple[str, str] = (database, index)
        if key not in cls._instances:
            cls._instances[key] = super(Singleton, cls).__call__(
                *args, **kwargs
            )
        return cls._instances[key]
