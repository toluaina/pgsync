"""PGSync plugin."""
import importlib
import inspect
import logging
import os
import pkgutil
from abc import ABC, abstractmethod
from typing import List, Optional

logger = logging.getLogger(__name__)


class Plugin(ABC):
    """Plugin base class."""

    @abstractmethod
    def transform(self, doc, **kwargs):
        """Must be implemented by all derived classes."""
        pass


class Plugins(object):
    def __init__(self, package: str, names: Optional[List] = None):
        self.package: str = package
        self.names: Optional[List] = names or []
        self.reload()

    def reload(self) -> None:
        """Reload the plugins from the available list."""
        self.plugins: List = []
        self._paths: List = []
        logger.debug(f"Reloading plugins from package: {self.package}")
        self.walk(self.package)

    def walk(self, package: str) -> None:
        """Recursively walk the supplied package and fetch all plugins"""
        plugins = importlib.import_module(package)
        for _, name, ispkg in pkgutil.iter_modules(
            plugins.__path__,
            f"{plugins.__name__}.",
        ):
            if ispkg:
                continue

            module = importlib.import_module(name)
            members = inspect.getmembers(module, inspect.isclass)
            for _, klass in members:
                if issubclass(klass, Plugin) & (klass is not Plugin):
                    if klass.name not in self.names:
                        continue
                    logger.debug(
                        f"Plugin class: {klass.__module__}.{klass.__name__}"
                    )
                    self.plugins.append(klass())

        paths: List = []
        if isinstance(plugins.__path__, str):
            paths.append(plugins.__path__)
        else:
            paths.extend([path for path in plugins.__path__])

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

    def transform(self, docs: List):
        """Apply all plugins to each doc."""
        for doc in docs:
            for plugin in self.plugins:
                logger.debug(f"Plugin: {plugin.name}")
                doc["_source"] = plugin.transform(
                    doc["_source"],
                    _id=doc["_id"],
                    _index=doc["_index"],
                )
            yield doc
