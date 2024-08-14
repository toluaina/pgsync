"""PGSync Plugin."""

import logging
import os
import sys
import typing as t
from abc import ABC, abstractmethod
from importlib import import_module
from inspect import getmembers, isclass
from pkgutil import iter_modules

logger = logging.getLogger(__name__)


class Plugin(ABC):
    """Plugin base class."""

    @abstractmethod
    def transform(self, doc: dict, **kwargs) -> dict:
        """This must be implemented by all derived classes."""
        pass


class Plugins(object):
    """
    A class representing a plugin.

    Args:
        package (str): The name of the package.
        names (list, optional): A list of names. Defaults to None.

    Attributes:
        package (str): The name of the package.
        names (list): A list of names.
    """

    def __init__(self, package: str, names: t.Optional[list] = None):
        self.package: str = package
        self.names: list = names or []
        self.reload()

    def reload(self) -> None:
        """Reloads the plugins from the available list."""
        self.plugins: list = []
        self._paths: list = []
        logger.debug(f"Reloading plugins from package: {self.package}")
        # skip in test
        if "test" not in sys.argv[0]:
            self.walk(self.package)

        # main plugin ordering
        self.plugins = sorted(
            self.plugins, key=lambda x: self.names.index(x.name)
        )

    def walk(self, package: str) -> None:
        """Recursively walk the supplied package and fetch all plugins."""
        module = import_module(package)
        for _, name, ispkg in iter_modules(
            module.__path__,
            prefix=f"{module.__name__}.",
        ):
            if ispkg:
                continue

            for _, klass in getmembers(import_module(name), isclass):
                if issubclass(klass, Plugin) & (klass is not Plugin):
                    if klass.name not in self.names:
                        continue
                    logger.debug(
                        f"Plugin class: {klass.__module__}.{klass.__name__}"
                    )
                    self.plugins.append(klass())

        paths: list = []
        if isinstance(module.__path__, str):
            paths.append(module.__path__)
        else:
            paths.extend([path for path in module.__path__])

        for pkg_path in paths:
            if pkg_path in self._paths:
                continue

            self._paths.append(pkg_path)
            for pkg in [
                path
                for path in os.listdir(pkg_path)
                if os.path.isdir(os.path.join(pkg_path, path))
            ]:
                self.walk(f"{package}.{pkg}")

    def transform(self, docs: list) -> t.Generator:
        """Applies all plugins to each doc."""
        for doc in docs:
            for plugin in self.plugins:
                doc["_source"] = plugin.transform(
                    doc["_source"],
                    _id=doc["_id"],
                    _index=doc["_index"],
                )
                if not doc["_source"]:
                    yield
            yield doc

    def auth(self, key: str) -> t.Optional[str]:
        """Get an auth value from a key."""
        for plugin in self.plugins:
            if hasattr(plugin, "auth"):
                try:
                    return plugin.auth(key)
                except Exception as e:
                    logger.exception(f"Error calling auth: {e}")
                    return None
        return None
