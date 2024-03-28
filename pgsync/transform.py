"""PGSync Transform."""

import logging
import typing as t

from .constants import (  # noqa
    CONCAT_TRANSFORM,
    RENAME_TRANSFORM,
    REPLACE_TRANSFORM,
)

logger = logging.getLogger(__name__)


class Transform(object):
    """Transform is really a builtin plugin"""

    @classmethod
    def rename(cls, data: dict, nodes: dict) -> dict:
        """Rename keys in a nested dictionary based on transform_node.
        "rename": {
            "id": "publisher_id",
            "name": "publisher_name"
        },
        """
        return cls._rename(data, cls.get(nodes, RENAME_TRANSFORM))

    @classmethod
    def _rename(
        cls, data: dict, nodes: dict, result: t.Optional[dict] = None
    ) -> dict:
        """
        Rename keys in a nested dictionary based on transform_node.

        example:
            "rename": {
                "id": "publisher_id",
                "name": "publisher_name"
            }

        Args:
            data (dict): The dictionary to rename keys in.
            nodes (dict): A dictionary containing the keys to rename and their new names.
            result (dict, optional): The resulting dictionary after renaming the keys. Defaults to None.

        Returns:
            dict: The resulting dictionary after renaming the keys.
        """
        result = result or {}
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(nodes.get(key), str):
                    key = nodes[key]
                elif isinstance(value, dict):
                    if key in nodes:
                        value = cls._rename(value, nodes[key])
                elif key in nodes.keys():
                    if isinstance(value, list):
                        value = [cls._rename(v, nodes[key]) for v in value]
                    elif isinstance(value, (str, int, float)):
                        if nodes[key]:
                            key = str(nodes[key])
                result[key] = value
        return result

    @classmethod
    def concat(cls, data: dict, nodes: dict) -> dict:
        """Concatenate column values into a new field
        {
            "columns": ["publisher_id", "publisher_name", "is_active", "foo"],
            "destination": "new_field",
            "delimiter": "-"
        },
        """
        return cls._concat(data, cls.get(nodes, CONCAT_TRANSFORM))

    @classmethod
    def _concat(
        cls, data: dict, nodes: dict, result: t.Optional[dict] = None
    ) -> dict:
        """Concatenate column values into a new field
        {
            "columns": ["publisher_id", "publisher_name", "is_active", "foo"],
            "destination": "new_field",
            "delimiter": "-"
        },
        """
        result = result or {}
        if isinstance(nodes, list):
            for node in nodes:
                cls._concat(data, node, result=result)

        if isinstance(data, dict):
            if "columns" in nodes:
                values: list = [data.get(key, key) for key in nodes["columns"]]
                delimiter: str = nodes.get("delimiter", "")
                destination: str = nodes["destination"]
                data[destination] = f"{delimiter}".join(
                    map(str, filter(None, values))
                )
            for key, value in data.items():
                if key in nodes:
                    if isinstance(value, dict):
                        value = cls._concat(value, nodes[key])
                    elif isinstance(value, list):
                        value = [
                            cls._concat(v, nodes[key])
                            for v in value
                            if key in nodes
                        ]
                result[key] = value
        return result

    """
    @classmethod
    def replace(cls, data: dict, nodes: dict) -> dict:
        # TODO!
        Replace field where value is
        "replace": {
            "code": {
                "-": "="
            }
        }
        return cls._replace(data, cls.get(nodes, REPLACE_TRANSFORM)))

    @classmethod
    def _replace(
        cls, data: dict, nodes: dict, result: Optional[dict] = None
    ) -> dict:
        # TODO!
        Replace field where value is
        "replace": {
            "code": {
                "-": "="
            }
        }
        result_dict = result_dict or {}
        if isinstance(data, dict):
            if nodes:
                for key, values in nodes.items():
                    if key not in data:
                        continue
                    if isinstance(data[key], list):
                        for k in values:
                            for search, replace in values[k].items():
                                data[key] = [
                                    x.replace(search, replace)
                                    for x in data[key]
                                ]
                    else:
                        for search, replace in values.items():
                            data[key] = data[key].replace(search, replace)

            for key, value in data.items():
                if isinstance(value, dict):
                    value = cls._replace(value, nodes.get(key))
                elif isinstance(value, list):
                    value = [
                        cls._replace(v, nodes[key])
                        for v in value
                        if key in nodes
                    ]
                result_dict[key] = value
        return result_dict
    """

    @classmethod
    def transform(cls, data: dict, nodes: dict):
        data = cls.rename(data, nodes)
        data = cls.concat(data, nodes)
        # data = cls.replace(data, nodes)
        return data

    @classmethod
    def get(cls, nodes: dict, type_: str) -> dict:
        transform_node: dict = {}
        if "transform" in nodes.keys():
            if type_ in nodes["transform"]:
                transform_node = nodes["transform"][type_]
        for child in nodes.get("children", {}):
            node: dict = cls.get(child, type_)
            if node:
                transform_node[child.get("label", child["table"])] = node
        return transform_node

    @classmethod
    def get_primary_keys(cls, primary_keys: dict) -> dict:
        """Get private keys entry from a nested dict."""

        def squash_list(values, _values=None):
            if not _values:
                _values = []
            if isinstance(values, dict):
                if len(values) == 1:
                    _values.append(values)
                else:
                    for key, value in values.items():
                        _values.extend(squash_list({key: value}))
            elif isinstance(values, list):
                for value in values:
                    _values.extend(squash_list(value))
            return _values

        target = []
        for values in squash_list(primary_keys):
            if len(values) > 1:
                for key, value in values.items():
                    target.append({key: value})
                continue
            target.append(values)

        target3 = []
        for values in target:
            for key, value in values.items():
                if isinstance(value, dict):
                    target3.append({key: value})
                elif isinstance(value, list):
                    _value: t.Dict[t.Any, t.Any] = {}
                    for v in value:
                        for _k, _v in v.items():
                            _value.setdefault(_k, [])
                            if isinstance(_v, list):
                                _value[_k].extend(_v)
                            else:
                                _value[_k].append(_v)
                    target3.append({key: _value})

        target4 = {}
        for values in target3:
            for key, value in values.items():
                if key not in target4:
                    target4[key] = {}
                for k, v in value.items():
                    if k not in target4[key]:
                        target4[key][k] = []
                    if isinstance(v, list):
                        for _v in v:
                            if _v not in target4[key][k]:
                                target4[key][k].append(_v)
                    else:
                        if v not in target4[key][k]:
                            target4[key][k].append(v)
                    target4[key][k] = sorted(target4[key][k])
        return target4
