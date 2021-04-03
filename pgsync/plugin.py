import inspect
import logging
import os
import pkgutil
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class Plugin(ABC):
    """Plugin base class."""

    @abstractmethod
    def transform(self, doc):
        """Must be implemented by all derived classes.
        """
        pass


class Plugins(object):

    def __init__(self, plugin_package, plugin_names=None):
        self.plugin_package = plugin_package
        self.plugin_names = plugin_names or []
        self.reload()

    def reload(self):
        """Reload the pluging from the available list."""
        self.plugins = []
        self._paths = []
        logger.debug(f'Reloading plugins from package: {self.plugin_package}')
        self.walk(self.plugin_package)

    def walk(self, package):
        """Recursively walk the supplied package and fetch all plugins
        """
        imported_package = __import__(package, fromlist=['foo'])
        for _, plugin_name, ispkg in pkgutil.iter_modules(
            imported_package.__path__,
            f'{imported_package.__name__}.',
        ):
            if ispkg:
                continue
            plugin_module = __import__(plugin_name, fromlist=['foo'])
            members = inspect.getmembers(plugin_module, inspect.isclass)
            for _, plugin_class in members:
                if issubclass(plugin_class, Plugin) & (
                    plugin_class is not Plugin
                ):
                    if plugin_class.name not in self.plugin_names:
                        continue
                    logger.debug(
                        f'Plugin class: {plugin_class.__module__}.'
                        f'{plugin_class.__name__}'
                    )
                    self.plugins.append(plugin_class())

        paths = []
        if isinstance(imported_package.__path__, str):
            paths.append(imported_package.__path__)
        else:
            paths.extend([path for path in imported_package.__path__])

        for pkg_path in paths:
            if pkg_path in self._paths:
                continue
            self._paths.append(pkg_path)
            for pkg in [
                path for path in os.listdir(pkg_path) if os.path.isdir(
                    os.path.join(pkg_path, path)
                )
            ]:
                self.walk(f'{package}.{pkg}')

    def transform(self, docs):
        """Apply all of the plugins to each doc."""
        for doc in docs:
            for plugin in self.plugins:
                logger.debug(f'Plugin: {plugin.name}')
                doc = plugin.transform(doc)
            yield doc
