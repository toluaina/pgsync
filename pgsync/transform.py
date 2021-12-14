"""PGSync transform."""
import logging
from typing import Optional

from six import string_types

from .constants import CONCAT_TRANSFORM, RENAME_TRANSFORM, REPLACE_TRANSFORM

logger = logging.getLogger(__name__)


def _get_transform(nodes: dict, key: str) -> dict:
    transform_node: dict = {}
    if "transform" in nodes.keys():
        if key in nodes["transform"]:
            transform_node = nodes["transform"][key]
    for child in nodes.get("children", {}):
        node: dict = _get_transform(child, key)
        if node:
            transform_node[child.get("label", child["table"])] = node
    return transform_node


def _rename_fields(data, nodes, result=None):
    """Rename keys in a nested dictionary based on transform_node."""
    result: dict = result or {}
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(nodes.get(key), str):
                key = nodes[key]
            elif isinstance(value, dict):
                if key in nodes:
                    value = _rename_fields(value, nodes[key])
            elif (
                isinstance(value, list)
                and value
                and not isinstance(
                    value[0],
                    dict,
                )
            ):
                try:
                    value = sorted(value)
                except TypeError:
                    pass
            elif key in nodes.keys():
                if isinstance(value, list):
                    value = [_rename_fields(v, nodes[key]) for v in value]
                elif isinstance(value, (string_types, int, float)):
                    if nodes[key]:
                        key = str(nodes[key])
            result[key] = value
    return result


def _concat_fields(data, nodes: dict, result: Optional[dict] = None) -> dict:
    """Concatenate fields."""
    result: dict = result or {}
    if isinstance(nodes, list):
        for node in nodes:
            _concat_fields(data, node, result=result)

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
                    value = _concat_fields(value, nodes[key])
                elif isinstance(value, list):
                    value = [
                        _concat_fields(
                            v,
                            nodes[key],
                        )
                        for v in value
                        if key in nodes
                    ]
            result[key] = value
    return result


# def _replace_fields(data, nodes, result_dict=None):
#     """Replace field values"""
#     result_dict = result_dict or {}
#     if isinstance(data, dict):
#         if nodes:
#             for key, values in nodes.items():
#                 if key not in data:
#                     continue
#                 if isinstance(data[key], list):
#                     for k in values:
#                         for search, replace in values[k].items():
#                             data[key] = [
#                                 x.replace(search, replace) for x in data[key]
#                             ]
#                 else:
#                     for search, replace in values.items():
#                         data[key] = data[key].replace(search, replace)

#         for key, value in data.items():
#             if isinstance(value, dict):
#                 value = _replace_fields(value, nodes.get(key))
#             elif isinstance(value, list):
#                 value = [
#                     _replace_fields(
#                         v,
#                         nodes[key],
#                     ) for v in value if key in nodes
#                 ]
#             result_dict[key] = value
#     return result_dict


def transform(data, nodes):
    transform_node = _get_transform(nodes, RENAME_TRANSFORM)
    data = _rename_fields(data, transform_node)
    transform_node = _get_transform(nodes, CONCAT_TRANSFORM)
    data = _concat_fields(data, transform_node)
    # transform_node = _get_transform(node, REPLACE_TRANSFORM)
    # _replace_fields(data, transform_node)
    return data


def get_private_keys(primary_keys):
    """
    Get private keys entry from a nested dict.
    re-write someday!
    """

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
                _value = {}
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
                    target4[key][k] = target4[key][k]
                else:
                    if v not in target4[key][k]:
                        target4[key][k].append(v)
            target4[key][k] = sorted(target4[key][k])
    return target4
