"""PGSync transform."""
import logging

from six import string_types

from .constants import CONCAT_TRANSFORM, RENAME_TRANSFORM, REPLACE_TRANSFORM

logger = logging.getLogger(__name__)


def _get_transform(node, key):
    transform_node = {}
    if 'transform' in node.keys():
        if key in node['transform']:
            transform_node = node['transform'][key]
    if 'children' in node.keys():
        for child in node['children']:
            label = child.get('label', child['table'])
            _transform_node = _get_transform(child, key)
            if _transform_node:
                transform_node[label] = _transform_node
    return transform_node


def _rename_fields(data, node, result=None):
    """Rename keys in a nested dictionary based on transform_node."""
    result = result or {}
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, dict):
                if key in node:
                    value = _rename_fields(value, node[key])
            elif isinstance(value, list) and value and not isinstance(
                value[0],
                dict,
            ):
                try:
                    value = sorted(value)
                except TypeError:
                    pass
            elif key in node.keys():
                if isinstance(value, list):
                    value = [
                        _rename_fields(v, node[key]) for v in value
                    ]
                elif isinstance(value, (string_types, int, float)):
                    if node[key]:
                        key = str(node[key])
            result[key] = value
    return result


def _concat_fields(data, node, result=None):
    """Concatenate fields."""
    result = result or {}
    if not node:
        return data
    if isinstance(data, dict):
        if 'columns' in node:
            columns = dict(
                filter(lambda x: x[0] in node['columns'], data.items())
            ).values()
            delimiter = node.get('delimiter', '')
            destination = node['destination']
            data[destination] = f'{delimiter}'.join(map(str, columns))
        for key, value in data.items():
            if isinstance(value, dict):
                value = _concat_fields(value, node[key])
            elif isinstance(value, list):
                value = [
                    _concat_fields(v, node[key]) for v in value if key in node
                ]
            result[key] = value
    return result


# def _replace_fields(data, node, result_dict=None):
#     """Replace field values"""
#     result_dict = result_dict or {}
#     if isinstance(data, dict):
#         if node:
#             for key, values in node.items():
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
#                 value = _replace_fields(value, node.get(key))
#             elif isinstance(value, list):
#                 value = [
#                     _replace_fields(
#                         v,
#                         node[key],
#                     ) for v in value if key in node
#                 ]
#             result_dict[key] = value
#     return result_dict


def transform(data, node):
    transform_node = _get_transform(node, RENAME_TRANSFORM)
    data = _rename_fields(data, transform_node)
    transform_node = _get_transform(node, CONCAT_TRANSFORM)
    data = _concat_fields(data, transform_node)
    # transform_node = _get_transform(node, REPLACE_TRANSFORM)
    # _replace_fields(data, transform_node)
    return data


def get_private_keys(primary_keys):
    """
    Get private keys entry from a nested dict.
    """
    def squash_list(values, _values=None):
        if not _values:
            _values = []
        if isinstance(values, dict):
            if len(values) == 1:
                _values.append(values)
            else:
                for key, value in values.items():
                    _values.extend(
                        squash_list({key: value})
                    )
        elif isinstance(values, list):
            for value in values:
                _values.extend(
                    squash_list(value)
                )
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
                    target4[key][k] = sorted(target4[key][k])

                else:
                    if v not in target4[key][k]:
                        target4[key][k].append(v)
    return target4
