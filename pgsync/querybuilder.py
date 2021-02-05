"""PGSync QueryBuilder."""
import sqlalchemy as sa

from .base import compiled_query, get_foreign_keys, get_primary_keys
from .constants import OBJECT, ONE_TO_MANY, ONE_TO_ONE, SCALAR
from .exc import FetchColumnForeignKeysError


class QueryBuilder(object):
    """Query builder."""

    def __init__(self, base, verbose=False):
        """
        Query builder constructor.

        :param base: base class
        :type base: `base.class`
        :param verbose: verbose
        :type verbose: `bool`
        """
        self.base = base
        self.verbose = verbose
        self.isouter = True

    def _get_foreign_keys(self, model_a, model_b):

        if model_a.through_tables or model_b.through_tables:

            if model_a.through_tables:
                through_tables = model_a.through_tables[0]

            if model_b.through_tables:
                through_tables = model_b.through_tables[0]
                model_a, model_b = model_b, model_a

            through = self.base.model(through_tables, model_a.schema)
            foreign_keys = get_foreign_keys(
                model_a.model,
                through,
            )
            for key, value in get_foreign_keys(
                through,
                model_b.model,
            ).items():
                if key in foreign_keys:
                    foreign_keys[key].extend(value)
                    continue
                foreign_keys[key] = value

        else:
            return get_foreign_keys(
                model_a.model,
                model_b.model,
            )
        return foreign_keys

    def _get_column_foreign_keys(
        self,
        columns,
        foreign_keys,
        table=None,
        schema=None,
    ):
        """
        Get the foreign keys where the columns are provided.

        e.g
            foreign_keys = {
                'schema.table_a': ['column_a', 'column_b', 'column_X'],
                'schema.table_b': ['column_x'],
                'schema.table_c': ['column_y']
            }
            columns = ['column_a', 'column_b']
            returns ['column_a', 'column_b']
        """
        column_names = [column.name for column in columns]
        if table is None:
            for table, cols in foreign_keys.items():
                if set(cols).issubset(set(column_names)):
                    return foreign_keys[table]
        else:
            # only return the intersection of columns that match
            if not table.startswith(f'{schema}.'):
                table = f'{schema}.{table}'
            for i, value in enumerate(foreign_keys[table]):
                if value not in columns:
                    foreign_keys[table].pop(i)
            return foreign_keys[table]

        msg = (
            f'No keys for columns: {columns} and foreign_keys: {foreign_keys}'
        )
        if table:
            msg += f' with table {table}'
        raise FetchColumnForeignKeysError(msg)

    def _get_child_keys(self, node, params):
        row = sa.cast(
            sa.func.JSON_BUILD_OBJECT(
                node.table,
                params,
            ),
            sa.dialects.postgresql.JSONB
        )
        for child in node.children:
            if (
                not child.parent.through_tables and
                child.parent.relationship_type == ONE_TO_MANY
            ):
                row = row.concat(
                    sa.cast(
                        sa.func.JSON_AGG(
                            child._subquery.c._keys
                        ),
                        sa.dialects.postgresql.JSONB,
                    )
                )
            else:
                row = row.concat(
                    sa.cast(
                        sa.func.JSON_BUILD_ARRAY(child._subquery.c._keys),
                        sa.dialects.postgresql.JSONB,
                    )
                )
        return row.label('_keys')

    def _root(self, node):
        columns = [
            sa.func.JSON_BUILD_ARRAY(
                *[
                    child._subquery.c._keys for child in node.children
                    if hasattr(
                        child._subquery.c,
                        '_keys',
                    )
                ]
            ),
            sa.func.JSON_BUILD_OBJECT(
                *node.columns
            ),
            *node.primary_keys,
        ]
        node._subquery = sa.select(columns)

        if self.from_obj is not None:
            node._subquery = node._subquery.select_from(
                self.from_obj
            )

        if node._filters:
            node._subquery = node._subquery.where(
                sa.and_(*node._filters)
            )
        node._subquery = node._subquery.alias()

    def _children(self, node):

        for child in node.children:

            onclause = []

            if child.through_tables:

                child.parent.columns.extend(
                    [
                        child.label,
                        getattr(child._subquery.c, child.label),
                    ]
                )

                through = self.base.model(
                    child.through_tables[0],
                    child.schema,
                )
                foreign_keys = get_foreign_keys(
                    child.parent.model,
                    through,
                )

                left_foreign_keys = self._get_column_foreign_keys(
                    child._subquery.columns,
                    foreign_keys,
                    table=through.original.name,
                    schema=child.schema,
                )
                right_foreign_keys = self._get_column_foreign_keys(
                    child.parent.model.columns,
                    foreign_keys,
                )

                for i in range(len(right_foreign_keys)):
                    onclause.append(
                        getattr(
                            child._subquery.c,
                            left_foreign_keys[i],
                        ) ==
                        getattr(
                            child.parent.model.c,
                            right_foreign_keys[i],
                        )
                    )

            else:

                child.parent.columns.extend(
                    [
                        child.label,
                        getattr(child._subquery.c, child.label),
                    ]
                )

                foreign_keys = get_foreign_keys(
                    node.model,
                    child.model,
                )
                left_foreign_keys = self._get_column_foreign_keys(
                    child._subquery.columns,
                    foreign_keys,
                )

                if left_foreign_keys == child.table:
                    right_foreign_keys = left_foreign_keys
                else:
                    right_foreign_keys = self._get_column_foreign_keys(
                        child.parent.model.columns,
                        foreign_keys,
                    )

                for i in range(len(left_foreign_keys)):
                    onclause.append(
                        getattr(
                            child._subquery.c,
                            left_foreign_keys[i],
                        ) ==
                        getattr(
                            child.parent.model.c,
                            right_foreign_keys[i],
                        )
                    )

            if self.from_obj is None:
                self.from_obj = child.parent.model

            if child._filters:
                self.isouter = False
                for _filter in child._filters:
                    if isinstance(_filter, sa.sql.elements.BinaryExpression):

                        for column in _filter._orig:
                            if hasattr(column, 'value'):
                                _column = child._subquery.c
                                if column._orig_key in node.table_columns:
                                    _column = node.model.c
                                if hasattr(_column, column._orig_key):
                                    onclause.append(
                                        getattr(
                                            _column,
                                            column._orig_key,
                                        ) == column.value
                                    )
                    elif isinstance(
                        _filter,
                        sa.sql.elements.BooleanClauseList,
                    ):
                        for clause in _filter.clauses:

                            for column in clause._orig:
                                if hasattr(column, 'value'):
                                    _column = child._subquery.c
                                    if column._orig_key in node.table_columns:
                                        _column = node.model.c
                                    if hasattr(_column, column._orig_key):
                                        onclause.append(
                                            getattr(
                                                _column,
                                                column._orig_key,
                                            ) == column.value
                                        )
                if self.verbose:
                    compiled_query(child._subquery, 'child._subquery')

            op = sa.and_
            if child.table == child.parent.table:
                op = sa.or_
            self.from_obj = self.from_obj.join(
                child._subquery,
                onclause=op(*onclause),
                isouter=self.isouter,
            )

    def _through(self, node):  # noqa: C901

        through = self.base.model(node.through_tables[0], node.schema)
        foreign_keys = get_foreign_keys(node.model, through)

        for key, value in get_foreign_keys(
            through,
            node.parent.model
        ).items():
            if key in foreign_keys:
                foreign_keys[key].extend(value)
                continue
            foreign_keys[key] = value

        foreign_key_columns = self._get_column_foreign_keys(
            node.model.columns,
            foreign_keys,
            table=node.table,
            schema=node.schema,
        )

        params = []
        for foreign_key_column in foreign_key_columns:
            params.append(
                sa.func.JSON_BUILD_OBJECT(
                    str(foreign_key_column),
                    sa.func.JSON_BUILD_ARRAY(
                        getattr(
                            node.model.c,
                            foreign_key_column,
                        )
                    )
                )
            )

        _keys = self._get_child_keys(
            node,
            sa.func.JSON_BUILD_ARRAY(
                 *params
            ).label('_keys')
        )

        columns = [_keys]
        # We need to include through keys and the actual keys
        if node.relationship_variant == SCALAR:
            columns.append(
                node.columns[1].label(
                    'anon'
                )
            )
        elif node.relationship_variant == OBJECT:

            if node.relationship_type == ONE_TO_ONE:

                if not node.children:
                    columns.append(
                        sa.func.JSON_BUILD_OBJECT(
                            node.columns[0],
                            node.columns[1],
                        ).label('anon')
                    )
                else:
                    columns.append(
                        sa.func.JSON_BUILD_OBJECT(
                            *node.columns
                        ).label('anon')
                    )
            elif node.relationship_type == ONE_TO_MANY:
                columns.append(
                    sa.func.JSON_BUILD_OBJECT(
                        *node.columns
                    ).label('anon')
                )

        columns.extend([
            getattr(
                node.model.c,
                foreign_key_column,
            ) for foreign_key_column in foreign_key_columns
        ])

        from_obj = None

        for child in node.children:

            onclause = []

            if child.through_tables:

                child_through = self.base.model(
                    child.through_tables[0],
                    child.schema,
                )
                child_foreign_keys = get_foreign_keys(
                    child.model,
                    child_through,
                )
                for key, value in get_foreign_keys(
                    child_through,
                    child.parent.model,
                ).items():
                    if key in child_foreign_keys:
                        child_foreign_keys[key].extend(value)
                        continue
                    child_foreign_keys[key] = value

                left_foreign_keys = self._get_column_foreign_keys(
                    child._subquery.columns,
                    child_foreign_keys,
                    table=child_through.original.name,
                    schema=child.schema,
                )

            else:

                child_foreign_keys = get_foreign_keys(
                    child.parent.model,
                    child.model,
                )
                left_foreign_keys = self._get_column_foreign_keys(
                    child._subquery.columns,
                    child_foreign_keys,
                )

            right_foreign_keys = child_foreign_keys[
                f'{child.parent.schema}.{child.parent.table}'
            ]

            for i in range(len(left_foreign_keys)):
                onclause.append(
                    getattr(
                        child._subquery.c,
                        left_foreign_keys[i],
                    ) ==
                    getattr(
                        child.parent.model.c,
                        right_foreign_keys[i],
                    )
                )

            if from_obj is None:
                from_obj = node.model

            if child._filters:
                self.isouter = False

                for _filter in child._filters:
                    if isinstance(_filter, sa.sql.elements.BinaryExpression):

                        for column in _filter._orig:
                            if hasattr(column, 'value'):
                                _column = child._subquery.c
                                if column._orig_key in node.table_columns:
                                    _column = node.model.c
                                if hasattr(
                                    _column,
                                    column._orig_key,
                                ):
                                    onclause.append(
                                        getattr(
                                            _column,
                                            column._orig_key,
                                        ) == column.value
                                    )
                    elif isinstance(
                        _filter,
                        sa.sql.elements.BooleanClauseList,
                    ):
                        for clause in _filter.clauses:

                            for column in clause._orig:
                                if hasattr(column, 'value'):
                                    _column = child._subquery.c
                                    if column._orig_key in node.table_columns:
                                        _column = node.model.c
                                    if hasattr(_column, column._orig_key):
                                        onclause.append(
                                            getattr(
                                                _column,
                                                column._orig_key,
                                            ) == column.value
                                        )

            from_obj = from_obj.join(
                child._subquery,
                onclause=sa.and_(*onclause),
                isouter=self.isouter,
            )

        outer_subquery = sa.select(columns)
        if node._filters:
            outer_subquery = outer_subquery.where(
                sa.and_(*node._filters)
            )

        if from_obj is not None:
            outer_subquery = outer_subquery.select_from(
                from_obj
            )

        outer_subquery = outer_subquery.alias()

        if self.verbose:
            compiled_query(outer_subquery, 'Outer subquery')

        through_primary_keys = get_primary_keys(through)

        params = []
        for through_primary_key in through_primary_keys:
            params.append(
                sa.func.JSON_BUILD_OBJECT(
                    str(through_primary_key),
                    sa.func.JSON_BUILD_ARRAY(
                        getattr(
                            through.c,
                            through_primary_key,
                        )
                    )
                )
            )

        through_keys = sa.cast(
            sa.func.JSON_BUILD_OBJECT(
                node.through_tables[0],
                sa.func.JSON_BUILD_ARRAY(
                    *params
                )
            ),
            sa.dialects.postgresql.JSONB
        )

        # book author through table
        _keys = sa.func.JSON_AGG(
            sa.cast(
                outer_subquery.c._keys,
                sa.dialects.postgresql.JSONB,
            ).concat(
                through_keys
            )
        ).label('_keys')

        left_foreign_keys = foreign_keys[f'{node.schema}.{node.table}']
        right_foreign_keys = self._get_column_foreign_keys(
            through.columns,
            foreign_keys,
            table=through.original.name,
            schema=node.schema,
        )

        columns = [
            _keys,
            sa.func.JSON_AGG(
                outer_subquery.c.anon
            ).label(node.label)
        ]

        foreign_keys = get_foreign_keys(node.parent.model, through)

        for column in foreign_keys[str(through.original)]:
            columns.append(
                getattr(through.c, column)
            )

        inner_subquery = sa.select(columns)

        if self.verbose:
            compiled_query(inner_subquery, 'Inner subquery')

        onclause = []
        for i in range(len(left_foreign_keys)):
            onclause.append(
                getattr(
                    outer_subquery.c,
                    left_foreign_keys[i],
                ) ==
                getattr(
                    through.c,
                    right_foreign_keys[i],
                )
            )

        if node._filters:
            self.isouter = False

        op = sa.and_
        if node.table == node.parent.table:
            op = sa.or_

        from_obj = through.join(
            outer_subquery,
            onclause=op(*onclause),
            isouter=self.isouter,
        )

        subquery = inner_subquery.select_from(
            from_obj
        )
        if node._filters:
            subquery = subquery.where(
                sa.and_(
                    *node._filters
                )
            )

        subquery = subquery.group_by(
            *[
                getattr(
                    through.c,
                    column,
                ) for column in foreign_keys[
                    str(through.original)
                ]
            ]
        )

        if self.verbose:
            compiled_query(subquery, 'Combined subquery')

        node._subquery = subquery.alias()

    def _non_through(self, node):  # noqa: C901

        from_obj = None

        for child in node.children:

            onclause = []

            foreign_keys = self._get_foreign_keys(node, child)

            foreign_key_columns = self._get_column_foreign_keys(
                child._subquery.columns,
                foreign_keys,
            )

            for i in range(len(foreign_key_columns)):
                onclause.append(
                    getattr(
                        child._subquery.c,
                        foreign_key_columns[i],
                    ) ==
                    getattr(
                        node.model.c,
                        foreign_keys[f'{node.schema}.{node.table}'][i],
                    )
                )

            if from_obj is None:
                from_obj = node.model

            if child._filters:
                self.isouter = False

                for _filter in child._filters:
                    if isinstance(_filter, sa.sql.elements.BinaryExpression):
                        for column in _filter._orig:
                            if hasattr(column, 'value'):
                                _column = child._subquery.c
                                if column._orig_key in node.table_columns:
                                    _column = node.model.c
                                if hasattr(_column, column._orig_key):
                                    onclause.append(
                                        getattr(
                                            _column,
                                            column._orig_key,
                                        ) == column.value
                                    )
                    elif isinstance(
                        _filter,
                        sa.sql.elements.BooleanClauseList,
                    ):
                        for clause in _filter.clauses:

                            for column in clause._orig:
                                if hasattr(column, 'value'):
                                    _column = child._subquery.c
                                    if column._orig_key in node.table_columns:
                                        _column = node.model.c
                                    if hasattr(_column, column._orig_key):
                                        onclause.append(
                                            getattr(
                                                _column,
                                                column._orig_key,
                                            ) == column.value
                                        )
            from_obj = from_obj.join(
                child._subquery,
                onclause=sa.and_(*onclause),
                isouter=self.isouter,
            )

        foreign_keys = get_foreign_keys(
            node.parent.model,
            node.model,
        )

        foreign_key_columns = self._get_column_foreign_keys(
            node.model.columns,
            foreign_keys,
            table=node.table,
            schema=node.schema,
        )

        params = []
        if node.parent.is_root:
            for primary_key in node.primary_keys:
                params.extend([
                    str(primary_key.name),
                    sa.func.JSON_BUILD_ARRAY(
                        getattr(
                            node.model.c,
                            primary_key.name,
                        )
                    )
                ])
        else:
            for primary_key in node.primary_keys:
                params.extend([
                    str(primary_key.name),
                    getattr(
                        node.model.c,
                        primary_key.name,
                    )
                ])

        if node.relationship_type == ONE_TO_ONE:
            _keys = self._get_child_keys(
                node,
                sa.func.JSON_BUILD_OBJECT(
                    *params
                )
            )
        elif node.relationship_type == ONE_TO_MANY:
            _keys = self._get_child_keys(
                node,
                sa.func.JSON_AGG(
                    sa.func.JSON_BUILD_OBJECT(
                        *params
                    )
                )
            )

        columns = [_keys]

        if node.relationship_variant == SCALAR:
            # TODO: Raise exception here if the number of columns > 1
            if node.relationship_type == ONE_TO_ONE:
                columns.append(
                    getattr(
                        node.model.c,
                        node.columns[0],
                    ).label(node.label)
                )
            elif node.relationship_type == ONE_TO_MANY:
                columns.append(
                    sa.func.JSON_AGG(
                        getattr(
                            node.model.c,
                            node.columns[0],
                        )
                    ).label(node.label)
                )
        elif node.relationship_variant == OBJECT:
            if node.relationship_type == ONE_TO_ONE:
                columns.append(
                    sa.func.JSON_BUILD_OBJECT(
                        *node.columns
                    ).label(node.label)
                )
            elif node.relationship_type == ONE_TO_MANY:
                columns.append(
                    sa.func.JSON_AGG(
                        sa.func.JSON_BUILD_OBJECT(
                            *node.columns
                        )
                    ).label(node.label)
                )

        for column in foreign_key_columns:
            columns.append(
                getattr(
                    node.model.c,
                    column,
                )
            )

        node._subquery = sa.select(columns)

        if from_obj is not None:
            node._subquery = node._subquery.select_from(
                from_obj
            )

        if node._filters:
            node._subquery = node._subquery.where(
                sa.and_(*node._filters)
            )

        if node.relationship_type == ONE_TO_MANY:
            node._subquery = node._subquery.group_by(
                *[
                    getattr(
                        node.model.c,
                        key,
                    ) for key in foreign_key_columns
                ]
            )

        node._subquery = node._subquery.alias()

    def build_queries(self, node):
        """Build node query."""
        self.from_obj = None

        # 1) add all child columns from one level below
        self._children(node)

        if node.is_root:
            self._root(node)
        else:
            # 2) subquery: these are for children creating their own columns
            if node.through_tables:
                self._through(node)
            else:
                self._non_through(node)
