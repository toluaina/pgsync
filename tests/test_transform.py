'''Transform tests.'''
import pytest

from pgsync.utils import transform


@pytest.mark.usefixtures('table_creator')
class TestTransform(object):
    '''Transform tests.'''

    def test_transform_rename(self):
        node = {
            'table': 'tableau',
            'columns': [
                'id',
                'code',
                'level',
            ],
            'children': [
                {
                    'table': 'child_1',
                    'columns': [
                        'column_1',
                        'column_2'
                    ],
                    'label': 'Child1',
                    'relationship': {
                        'variant': 'object',
                        'type': 'one_to_one',
                    },
                    'transform': {
                        'rename': {
                            'column_1': 'column1'
                        }
                    }
                },
                {
                    'table': 'Child2',
                    'columns': [
                        'column_1',
                        'column_2'
                    ],
                    'label': 'Child2',
                    'relationship': {
                        'variant': 'object',
                        'type': 'one_to_many',
                    },
                    'transform': {
                        'rename': {
                            'column_2': 'column2'
                        }
                    }
                }
            ],
            'transform': {
                'rename': {
                    'id': 'myId',
                    'code': 'myCode',
                    'level': 'Level'
                }
            }
        }

        row = {
            'level': 1,
            'id': '007',
            'code': 'be',
            'Child1': [
                {'column_1': 2, 'column_2': 'aa'},
                {'column_1': 3, 'column_2': 'bb'}
            ],
            'Child2': [
                {'column_1': 2, 'column_2': 'aa'},
                {'column_1': 3, 'column_2': 'bb'}
            ],
        }
        transformed = transform(None, row, node)
        assert transformed == {
            'Child1': [
                {'column1': 2, 'column_2': 'aa'},
                {'column1': 3, 'column_2': 'bb'},
            ],
            'Child2': [
                {'column2': 'aa', 'column_1': 2},
                {'column2': 'bb', 'column_1': 3}
            ],
            'Level': 1,
            'myCode': 'be',
            'myId': '007',
        }
