"""PGSync transform."""
import logging

from six import string_types

from .constants import CONCAT_TRANSFORM, RENAME_TRANSFORM

logger = logging.getLogger(__name__)


def rename_fields(fields, transform_node, result_dict=None):
    """Rename keys in a nested dictionary based on transform_node."""
    result_dict = result_dict or {}
    if isinstance(fields, dict):
        for key, value in fields.items():
            if isinstance(value, dict):
                if key in transform_node:
                    value = rename_fields(value, transform_node[key])
            elif isinstance(value, list) and value and not isinstance(
                value[0],
                dict,
            ):
                try:
                    value = sorted(value)
                except TypeError:
                    pass
            elif key in transform_node.keys():
                if isinstance(value, list):
                    value = [
                        rename_fields(v, transform_node[key]) for v in value
                    ]
                elif isinstance(value, (string_types, int, float)):
                    if transform_node[key]:
                        key = str(transform_node[key])
            result_dict[key] = value
    return result_dict


def concat_fields(fields, transform_node, result_dict=None):
    """Concatenate fields of a column."""
    result_dict = result_dict or {}
    if not transform_node:
        return fields
    if isinstance(fields, dict):
        if 'columns' in transform_node:
            columns = dict(
                filter(
                    lambda x: x[0] in transform_node['columns'],
                    fields.items(),
                )
            ).values()
            delimiter = transform_node.get('delimiter', '')
            destination = transform_node['destination']
            fields[destination] = f'{delimiter}'.join(map(str, columns))
        for key, value in fields.items():
            if isinstance(value, dict):
                value = concat_fields(value, transform_node[key])
            elif isinstance(value, list):
                value = [
                    concat_fields(
                        v,
                        transform_node[key],
                    ) for v in value if key in transform_node
                ]
            result_dict[key] = value
    return result_dict


def get_transform(node, name):
    transform_node = {}
    if 'transform' in node.keys():
        if name in node['transform']:
            transform_node = node['transform'][name]
    if 'children' in node.keys():
        for child in node['children']:
            label = child.get('label', child['table'])
            _transform_node = get_transform(child, name)
            if _transform_node:
                transform_node[label] = _transform_node
    return transform_node


def transform(row, node):
    transform_node = get_transform(node, RENAME_TRANSFORM)
    row = rename_fields(row, transform_node)
    transform_node = get_transform(node, CONCAT_TRANSFORM)
    return concat_fields(row, transform_node)


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
