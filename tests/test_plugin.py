"""Plugin tests."""

import typing as t
from unittest.mock import MagicMock, patch

import pytest

from pgsync.plugin import Plugin, Plugins


class TestPlugin:
    """Tests for the Plugin abstract base class."""

    def test_plugin_is_abstract(self):
        """Test that Plugin cannot be instantiated directly."""
        with pytest.raises(TypeError) as excinfo:
            Plugin()
        assert "abstract" in str(excinfo.value).lower()

    def test_plugin_subclass_must_implement_transform(self):
        """Test that subclass without transform implementation raises error."""

        class IncompletePlugin(Plugin):
            pass

        with pytest.raises(TypeError) as excinfo:
            IncompletePlugin()
        assert "abstract" in str(excinfo.value).lower()

    def test_plugin_subclass_with_transform(self):
        """Test that subclass with transform can be instantiated."""

        class ValidPlugin(Plugin):
            name = "valid_plugin"

            def transform(self, doc: dict, **kwargs: t.Any) -> dict:
                doc["processed"] = True
                return doc

        plugin = ValidPlugin()
        assert hasattr(plugin, "transform")
        assert plugin.name == "valid_plugin"

    def test_plugin_transform_modifies_doc(self):
        """Test that transform method correctly modifies document."""

        class ModifyPlugin(Plugin):
            name = "modify_plugin"

            def transform(self, doc: dict, **kwargs: t.Any) -> dict:
                doc["modified"] = True
                doc["kwargs"] = kwargs
                return doc

        plugin = ModifyPlugin()
        doc = {"field": "value"}
        result = plugin.transform(doc, _id="123", _index="test")

        assert result["field"] == "value"
        assert result["modified"] is True
        assert result["kwargs"]["_id"] == "123"
        assert result["kwargs"]["_index"] == "test"


class TestPlugins:
    """Tests for the Plugins class."""

    def test_plugins_init_with_empty_names(self):
        """Test Plugins initialization with empty names list."""
        plugins = Plugins("fake_package", names=[])
        assert plugins.package == "fake_package"
        assert plugins.names == []
        assert plugins.plugins == []

    def test_plugins_init_with_none_names(self):
        """Test Plugins initialization with None names."""
        plugins = Plugins("fake_package", names=None)
        assert plugins.names == []

    def test_plugins_reload_clears_plugins(self):
        """Test that reload clears existing plugins."""
        plugins = Plugins("fake_package", names=[])
        plugins.plugins = [MagicMock()]
        plugins._paths = ["/some/path"]

        plugins.reload()

        assert plugins.plugins == []
        assert plugins._paths == []

    @patch("pgsync.plugin.logger")
    def test_plugins_reload_logs_debug(self, mock_logger):
        """Test that reload logs debug message."""
        plugins = Plugins("fake_package", names=[])
        plugins.reload()
        mock_logger.debug.assert_called_with(
            "Reloading plugins from package: fake_package"
        )

    def test_plugins_transform_with_no_plugins(self):
        """Test transform with empty plugins list."""
        plugins = Plugins("fake_package", names=[])
        docs = [
            {"_id": "1", "_index": "test", "_source": {"field": "value"}},
        ]

        result = list(plugins.transform(docs))
        assert len(result) == 1
        assert result[0]["_source"]["field"] == "value"

    def test_plugins_transform_applies_single_plugin(self):
        """Test transform applies a single plugin."""

        class TestPlugin(Plugin):
            name = "test_plugin"

            def transform(self, doc: dict, **kwargs: t.Any) -> dict:
                doc["transformed"] = True
                return doc

        plugins = Plugins("fake_package", names=["test_plugin"])
        plugins.plugins = [TestPlugin()]

        docs = [
            {"_id": "1", "_index": "test", "_source": {"field": "value"}},
        ]

        result = list(plugins.transform(docs))
        assert len(result) == 1
        assert result[0]["_source"]["transformed"] is True
        assert result[0]["_source"]["field"] == "value"

    def test_plugins_transform_applies_multiple_plugins_in_order(self):
        """Test transform applies multiple plugins in order."""

        class FirstPlugin(Plugin):
            name = "first_plugin"

            def transform(self, doc: dict, **kwargs: t.Any) -> dict:
                doc["order"] = doc.get("order", [])
                doc["order"].append("first")
                return doc

        class SecondPlugin(Plugin):
            name = "second_plugin"

            def transform(self, doc: dict, **kwargs: t.Any) -> dict:
                doc["order"] = doc.get("order", [])
                doc["order"].append("second")
                return doc

        plugins = Plugins(
            "fake_package", names=["first_plugin", "second_plugin"]
        )
        plugins.plugins = [FirstPlugin(), SecondPlugin()]

        docs = [
            {"_id": "1", "_index": "test", "_source": {}},
        ]

        result = list(plugins.transform(docs))
        assert result[0]["_source"]["order"] == ["first", "second"]

    def test_plugins_transform_yields_none_when_source_empty(self):
        """Test transform yields None when plugin returns empty source."""

        class EmptyPlugin(Plugin):
            name = "empty_plugin"

            def transform(self, doc: dict, **kwargs: t.Any) -> dict:
                return {}

        plugins = Plugins("fake_package", names=["empty_plugin"])
        plugins.plugins = [EmptyPlugin()]

        docs = [
            {"_id": "1", "_index": "test", "_source": {"field": "value"}},
        ]

        result = list(plugins.transform(docs))
        # Should have two yields - one None (from empty source), one doc
        assert len(result) == 2
        assert result[0] is None

    def test_plugins_transform_passes_id_and_index(self):
        """Test transform passes _id and _index to plugins."""

        received_kwargs = {}

        class RecordPlugin(Plugin):
            name = "record_plugin"

            def transform(self, doc: dict, **kwargs: t.Any) -> dict:
                received_kwargs.update(kwargs)
                return doc

        plugins = Plugins("fake_package", names=["record_plugin"])
        plugins.plugins = [RecordPlugin()]

        docs = [
            {"_id": "doc-123", "_index": "my-index", "_source": {}},
        ]

        list(plugins.transform(docs))
        assert received_kwargs["_id"] == "doc-123"
        assert received_kwargs["_index"] == "my-index"

    def test_plugins_auth_with_no_plugins(self):
        """Test auth returns None when no plugins."""
        plugins = Plugins("fake_package", names=[])
        result = plugins.auth("api_key")
        assert result is None

    def test_plugins_auth_with_plugin_without_auth(self):
        """Test auth returns None when plugin has no auth method."""

        class NoAuthPlugin(Plugin):
            name = "no_auth_plugin"

            def transform(self, doc: dict, **kwargs: t.Any) -> dict:
                return doc

        plugins = Plugins("fake_package", names=["no_auth_plugin"])
        plugins.plugins = [NoAuthPlugin()]

        result = plugins.auth("api_key")
        assert result is None

    def test_plugins_auth_with_plugin_with_auth(self):
        """Test auth returns value from plugin auth method."""

        class AuthPlugin(Plugin):
            name = "auth_plugin"

            def transform(self, doc: dict, **kwargs: t.Any) -> dict:
                return doc

            def auth(self, key: str) -> str:
                return f"secret_{key}"

        plugins = Plugins("fake_package", names=["auth_plugin"])
        plugins.plugins = [AuthPlugin()]

        result = plugins.auth("api_key")
        assert result == "secret_api_key"

    @patch("pgsync.plugin.logger")
    def test_plugins_auth_logs_exception(self, mock_logger):
        """Test auth logs exception when plugin auth fails."""

        class FailingAuthPlugin(Plugin):
            name = "failing_auth_plugin"

            def transform(self, doc: dict, **kwargs: t.Any) -> dict:
                return doc

            def auth(self, key: str) -> str:
                raise ValueError("Auth failed")

        plugins = Plugins("fake_package", names=["failing_auth_plugin"])
        plugins.plugins = [FailingAuthPlugin()]

        result = plugins.auth("api_key")
        assert result is None
        mock_logger.exception.assert_called_once()

    def test_plugins_auth_stops_at_first_plugin_with_auth(self):
        """Test auth returns from first plugin with auth method."""

        class FirstAuthPlugin(Plugin):
            name = "first_auth"

            def transform(self, doc: dict, **kwargs: t.Any) -> dict:
                return doc

            def auth(self, key: str) -> str:
                return "first"

        class SecondAuthPlugin(Plugin):
            name = "second_auth"

            def transform(self, doc: dict, **kwargs: t.Any) -> dict:
                return doc

            def auth(self, key: str) -> str:
                return "second"

        plugins = Plugins("fake_package", names=["first_auth", "second_auth"])
        plugins.plugins = [FirstAuthPlugin(), SecondAuthPlugin()]

        result = plugins.auth("key")
        assert result == "first"

    @patch("pgsync.plugin.import_module")
    @patch("pgsync.plugin.iter_modules")
    def test_plugins_walk_skips_packages(
        self, mock_iter_modules, mock_import_module
    ):
        """Test walk skips packages (ispkg=True)."""
        mock_module = MagicMock()
        mock_module.__path__ = ["/fake/path"]
        mock_module.__name__ = "fake_module"
        mock_import_module.return_value = mock_module

        # Return a package (ispkg=True) which should be skipped
        mock_iter_modules.return_value = [
            (None, "fake_module.subpkg", True),  # This is a package
        ]

        # Mock os.listdir to return empty (no subdirs)
        with patch("os.listdir", return_value=[]):
            plugins = Plugins.__new__(Plugins)
            plugins.plugins = []
            plugins._paths = []
            plugins.names = []

            plugins.walk("fake_module")

            # Should not have added any plugins since it was a package
            assert plugins.plugins == []

    @patch("pgsync.plugin.import_module")
    @patch("pgsync.plugin.iter_modules")
    @patch("pgsync.plugin.getmembers")
    def test_plugins_walk_loads_plugin_class(
        self, mock_getmembers, mock_iter_modules, mock_import_module
    ):
        """Test walk loads plugin classes from modules."""

        class MyPlugin(Plugin):
            name = "my_plugin"

            def transform(self, doc: dict, **kwargs: t.Any) -> dict:
                return doc

        mock_module = MagicMock()
        mock_module.__path__ = ["/fake/path"]
        mock_module.__name__ = "fake_module"
        mock_import_module.return_value = mock_module

        # Return a module (ispkg=False)
        mock_iter_modules.return_value = [
            (None, "fake_module.my_plugin", False),
        ]

        # Return the plugin class when inspecting members
        mock_getmembers.return_value = [("MyPlugin", MyPlugin)]

        with patch("os.listdir", return_value=[]):
            plugins = Plugins.__new__(Plugins)
            plugins.plugins = []
            plugins._paths = []
            plugins.names = ["my_plugin"]

            plugins.walk("fake_module")

            # Should have loaded the plugin
            assert len(plugins.plugins) == 1
            assert isinstance(plugins.plugins[0], MyPlugin)

    @patch("pgsync.plugin.import_module")
    @patch("pgsync.plugin.iter_modules")
    @patch("pgsync.plugin.getmembers")
    def test_plugins_walk_skips_unlisted_plugins(
        self, mock_getmembers, mock_iter_modules, mock_import_module
    ):
        """Test walk skips plugins not in names list."""

        class UnlistedPlugin(Plugin):
            name = "unlisted_plugin"

            def transform(self, doc: dict, **kwargs: t.Any) -> dict:
                return doc

        mock_module = MagicMock()
        mock_module.__path__ = ["/fake/path"]
        mock_module.__name__ = "fake_module"
        mock_import_module.return_value = mock_module

        mock_iter_modules.return_value = [
            (None, "fake_module.unlisted", False),
        ]

        mock_getmembers.return_value = [("UnlistedPlugin", UnlistedPlugin)]

        with patch("os.listdir", return_value=[]):
            plugins = Plugins.__new__(Plugins)
            plugins.plugins = []
            plugins._paths = []
            plugins.names = ["some_other_plugin"]  # Not "unlisted_plugin"

            plugins.walk("fake_module")

            # Should not have loaded the plugin since it's not in names
            assert plugins.plugins == []

    def test_plugins_walk_handles_string_path(self):
        """Test walk handles module with string __path__."""
        with patch("pgsync.plugin.import_module") as mock_import:
            mock_module = MagicMock()
            mock_module.__path__ = "/single/string/path"
            mock_module.__name__ = "fake_module"
            mock_import.return_value = mock_module

            with patch("pgsync.plugin.iter_modules", return_value=[]):
                with patch("os.listdir", return_value=[]):
                    plugins = Plugins.__new__(Plugins)
                    plugins.plugins = []
                    plugins._paths = []
                    plugins.names = []

                    plugins.walk("fake_module")

                    # Path should be added
                    assert "/single/string/path" in plugins._paths

    def test_plugins_walk_skips_already_visited_paths(self):
        """Test walk skips paths that have already been visited."""
        with patch("pgsync.plugin.import_module") as mock_import:
            mock_module = MagicMock()
            mock_module.__path__ = ["/already/visited"]
            mock_module.__name__ = "fake_module"
            mock_import.return_value = mock_module

            with patch("pgsync.plugin.iter_modules", return_value=[]):
                with patch("os.listdir") as mock_listdir:
                    mock_listdir.return_value = []

                    plugins = Plugins.__new__(Plugins)
                    plugins.plugins = []
                    plugins._paths = ["/already/visited"]  # Already visited
                    plugins.names = []

                    plugins.walk("fake_module")

                    # os.listdir should not be called since path was already visited
                    mock_listdir.assert_not_called()
