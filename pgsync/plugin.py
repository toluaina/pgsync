"""PGSync plugin."""
import logging
import os
from abc import ABC, abstractmethod
from importlib import import_module
from inspect import getmembers, isclass
from pkgutil import iter_modules
from typing import Optional

logger = logging.getLogger(__name__)


class Plugin(ABC):
    """Plugin base class."""

    @abstractmethod
    def transform(self, doc: dict, **kwargs) -> dict:
        """Must be implemented by all derived classes."""
        pass


class Plugins(object):
    def __init__(self, package: str, names: Optional[list] = None):
        self.package: str = package
        self.names: list = names or []
        self.reload()

    def reload(self) -> None:
        """Reload the plugins from the available list."""
        self.plugins: list = []
        self._paths: list = []
        logger.debug(f"Reloading plugins from package: {self.package}")
        self.walk(self.package)

    def walk(self, package: str) -> None:
        """Recursively walk the supplied package and fetch all plugins"""
        plugins = import_module(package)
        for _, name, ispkg in iter_modules(
            plugins.__path__,
            f"{plugins.__name__}.",
        ):
            if ispkg:
                continue

            module = import_module(name)
            members = getmembers(module, isclass)
            for _, klass in members:
                if issubclass(klass, Plugin) & (klass is not Plugin):
                    if klass.name not in self.names:
                        continue
                    logger.debug(
                        f"Plugin class: {klass.__module__}.{klass.__name__}"
                    )
                    self.plugins.append(klass())

        paths: list = []
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

    def transform(self, docs: list) -> dict:
        """Apply all plugins to each doc."""
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

    def auth(self, key: str) -> Optional[str]:
        """Get an auth value from a key."""
        for plugin in self.plugins:
            if hasattr(plugin, "auth"):
                try:
                    return plugin.auth(key)
                except Exception as e:
                    logger.exception(f"Error calling auth: {e}")
                    return None
