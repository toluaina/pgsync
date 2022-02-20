"""PGSync QueryBuilder."""
from typing import Dict, List

import sqlalchemy as sa

from .base import Base, compiled_query, get_foreign_keys
from .constants import OBJECT, ONE_TO_MANY, ONE_TO_ONE, SCALAR
from .exc import FetchColumnForeignKeysError
from .node import Node, node_from_table


class QueryBuilder(object):
    """Query builder."""

    def __init__(self, base: Base, verbose: bool = False):
        """Query builder constructor."""
        self.base: Base = base
        self.verbose: bool = verbose
        self.isouter: bool = True

    def _json_build_object(
        self, columns: list
    ) -> sa.sql.elements.BinaryExpression:
        """
        Tries to get aroud the limitation of JSON_BUILD_OBJECT which
        has a default limit of 100 args.
        This results in the error: 'cannot pass more than 100 arguments
        to a function'

        with the 100 arguments limit this implies we can only select 50 columns
        at a time.
        """
        i: int = 0
        chunk_size: int = 100
        expression: sa.sql.elements.BinaryExpression = None
        while i < len(columns):
            chunk = columns[i : i + chunk_size]
            if i == 0:
                expression = sa.cast(
                    sa.func.JSON_BUILD_OBJECT(*chunk),
                    sa.dialects.postgresql.JSONB,
                )
            else:
                expression = expression.concat(
                    sa.cast(
                        sa.func.JSON_BUILD_OBJECT(*chunk),
                        sa.dialects.postgresql.JSONB,
                    )
                )
            i += chunk_size

        if expression is None:
            raise RuntimeError("invalid expression")

        return expression

    def _get_foreign_keys(self, node_a: Node, node_b: Node) -> Dict:
        if (
            node_a.relationship.through_tables
            or node_b.relationship.through_tables
        ):

            if node_a.relationship.through_tables:
                through_table: str = node_a.relationship.through_tables[0]

            if node_b.relationship.through_tables:
                through_table: str = node_b.relationship.through_tables[0]
                node_a, node_b = node_b, node_a

            through: Node = node_from_table(
                self.base,
                through_table,
                node_a.schema,
            )
            foreign_keys: dict = get_foreign_keys(node_a, through)
            for key, value in get_foreign_keys(through, node_b).items():
                if key in foreign_keys:
                    foreign_keys[key].extend(value)
                    continue
                foreign_keys[key] = value

            return foreign_keys

        else:

            return get_foreign_keys(node_a, node_b)

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
        # TODO: normalize this elsewhere
        try:
            column_names: List = [column.name for column in columns]
        except AttributeError:
            column_names: List = [column for column in columns]

        if table is None:
            for table, cols in foreign_keys.items():
                if set(cols).issubset(set(column_names)):
                    return foreign_keys[table]
        else:
            # only return the intersection of columns that match
            if not table.startswith(f"{schema}."):
                table = f"{schema}.{table}"
            for i, value in enumerate(foreign_keys[table]):
                if value not in columns:
                    foreign_keys[table].pop(i)
            return foreign_keys[table]

        msg: str = (
            f"No keys for columns: {columns} and foreign_keys: {foreign_keys}"
        )
        if table:
            msg += f" with table {table}"
        raise FetchColumnForeignKeysError(msg)

    def _get_child_keys(self, node: Node, params: dict):
        row = sa.cast(
            sa.func.JSON_BUILD_OBJECT(
                node.table,
                params,
            ),
            sa.dialects.postgresql.JSONB,
        )
        for child in node.children:
            if (
                not child.parent.relationship.through_tables
                and child.parent.relationship.type == ONE_TO_MANY
            ):
                row = row.concat(
                    sa.cast(
                        sa.func.JSON_AGG(child._subquery.c._keys),
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
        return row.label("_keys")

    def _root(self, node) -> None:
        columns = [
            sa.func.JSON_BUILD_ARRAY(
                *[
                    child._subquery.c._keys
                    for child in node.children
                    if hasattr(
                        child._subquery.c,
                        "_keys",
                    )
                ]
            ),
            self._json_build_object(node.columns),
            *node.primary_keys,
        ]
        node._subquery = sa.select(columns)

        if self.from_obj is not None:
            node._subquery = node._subquery.select_from(self.from_obj)

        if node._filters:
            node._subquery = node._subquery.where(sa.and_(*node._filters))
        node._subquery = node._subquery.alias()

        if not node.is_root:
            node._subquery = node._subquery.lateral()

    def _children(self, node: Node) -> None:

        for child in node.children:

            onclause: List = []

            if child.relationship.through_tables:

                child.parent.columns.extend(
                    [
                        child.label,
                        child._subquery.c[child.label],
                    ]
                )

                through = node_from_table(
                    self.base,
                    child.relationship.through_tables[0],
                    child.schema,
                )
                foreign_keys = get_foreign_keys(child.parent, through)

                left_foreign_keys = self._get_column_foreign_keys(
                    child._subquery.columns,
                    foreign_keys,
                    table=through.name,
                    schema=child.schema,
                )
                right_foreign_keys = self._get_column_foreign_keys(
                    child.parent.model.columns,
                    foreign_keys,
                )

                for i in range(len(right_foreign_keys)):
                    onclause.append(
                        child._subquery.c[left_foreign_keys[i]]
                        == child.parent.model.c[right_foreign_keys[i]]
                    )

            else:

                child.parent.columns.extend(
                    [
                        child.label,
                        child._subquery.c[child.label],
                    ]
                )

                foreign_keys = get_foreign_keys(node, child)
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
                        child._subquery.c[left_foreign_keys[i]]
                        == child.parent.model.c[right_foreign_keys[i]]
                    )

            if self.from_obj is None:
                self.from_obj = child.parent.model

            if child._filters:
                self.isouter = False
                for _filter in child._filters:
                    if isinstance(_filter, sa.sql.elements.BinaryExpression):

                        for column in _filter._orig:
                            if hasattr(column, "value"):
                                _column = child._subquery.c
                                if column._orig_key in node.table_columns:
                                    _column = node.model.c
                                if hasattr(_column, column._orig_key):
                                    onclause.append(
                                        _column[column._orig_key]
                                        == column.value
                                    )
                    elif isinstance(
                        _filter,
                        sa.sql.elements.BooleanClauseList,
                    ):
                        for clause in _filter.clauses:

                            for column in clause._orig:
                                if hasattr(column, "value"):
                                    _column = child._subquery.c
                                    if column._orig_key in node.table_columns:
                                        _column = node.model.c
                                    if hasattr(_column, column._orig_key):
                                        onclause.append(
                                            _column[column._orig_key]
                                            == column.value
                                        )
                if self.verbose:
                    compiled_query(child._subquery, "child._subquery")

            op = sa.and_
            if child.table == child.parent.table:
                op = sa.or_
            self.from_obj = self.from_obj.join(
                child._subquery,
                onclause=op(*onclause),
                isouter=self.isouter,
            )

    def _through(self, node: Node) -> None:  # noqa: C901

        through = node_from_table(
            self.base,
            node.relationship.through_tables[0],
            node.schema,
        )
        foreign_keys = get_foreign_keys(node, through)

        for key, value in get_foreign_keys(through, node.parent).items():
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

        params: list = []
        for foreign_key_column in foreign_key_columns:
            params.append(
                sa.func.JSON_BUILD_OBJECT(
                    str(foreign_key_column),
                    sa.func.JSON_BUILD_ARRAY(node.model.c[foreign_key_column]),
                )
            )

        _keys = self._get_child_keys(
            node, sa.func.JSON_BUILD_ARRAY(*params).label("_keys")
        )

        columns = [_keys]
        # We need to include through keys and the actual keys
        if node.relationship.variant == SCALAR:
            columns.append(node.columns[1].label("anon"))
        elif node.relationship.variant == OBJECT:

            if node.relationship.type == ONE_TO_ONE:

                if not node.children:
                    columns.append(
                        sa.func.JSON_BUILD_OBJECT(
                            node.columns[0],
                            node.columns[1],
                        ).label("anon")
                    )
                else:
                    columns.append(
                        self._json_build_object(node.columns).label("anon")
                    )
            elif node.relationship.type == ONE_TO_MANY:
                columns.append(
                    self._json_build_object(node.columns).label("anon")
                )

        columns.extend(
            [
                node.model.c[foreign_key_column]
                for foreign_key_column in foreign_key_columns
            ]
        )

        from_obj = None

        for child in node.children:

            onclause = []

            if child.relationship.through_tables:

                child_through = node_from_table(
                    self.base,
                    child.relationship.through_tables[0],
                    child.schema,
                )
                child_foreign_keys = get_foreign_keys(child, child_through)
                for key, value in get_foreign_keys(
                    child_through,
                    child.parent,
                ).items():
                    if key in child_foreign_keys:
                        child_foreign_keys[key].extend(value)
                        continue
                    child_foreign_keys[key] = value

                left_foreign_keys = self._get_column_foreign_keys(
                    child._subquery.columns,
                    child_foreign_keys,
                    table=child_through.table,
                    schema=child.schema,
                )

            else:

                child_foreign_keys = get_foreign_keys(child.parent, child)
                left_foreign_keys = self._get_column_foreign_keys(
                    child._subquery.columns,
                    child_foreign_keys,
                )

            right_foreign_keys = child_foreign_keys[child.parent.name]

            for i in range(len(left_foreign_keys)):
                onclause.append(
                    child._subquery.c[left_foreign_keys[i]]
                    == child.parent.model.c[right_foreign_keys[i]]
                )

            if from_obj is None:
                from_obj = node.model

            if child._filters:
                self.isouter = False

                for _filter in child._filters:
                    if isinstance(_filter, sa.sql.elements.BinaryExpression):

                        for column in _filter._orig:
                            if hasattr(column, "value"):
                                _column = child._subquery.c
                                if column._orig_key in node.table_columns:
                                    _column = node.model.c
                                if hasattr(
                                    _column,
                                    column._orig_key,
                                ):
                                    onclause.append(
                                        _column[column._orig_key]
                                        == column.value
                                    )
                    elif isinstance(
                        _filter,
                        sa.sql.elements.BooleanClauseList,
                    ):
                        for clause in _filter.clauses:

                            for column in clause._orig:
                                if hasattr(column, "value"):
                                    _column = child._subquery.c
                                    if column._orig_key in node.table_columns:
                                        _column = node.model.c
                                    if hasattr(_column, column._orig_key):
                                        onclause.append(
                                            _column[column._orig_key]
                                            == column.value
                                        )

            from_obj = from_obj.join(
                child._subquery,
                onclause=sa.and_(*onclause),
                isouter=self.isouter,
            )

        outer_subquery = sa.select(columns)

        parent_foreign_key_columns = self._get_column_foreign_keys(
            through.columns,
            foreign_keys,
            schema=node.schema,
        )
        where: list = []
        for i in range(len(foreign_key_columns)):
            where.append(
                node.model.c[foreign_key_columns[i]]
                == through.model.c[parent_foreign_key_columns[i]]
            )
        outer_subquery = outer_subquery.where(sa.and_(*where))

        if node._filters:
            outer_subquery = outer_subquery.where(sa.and_(*node._filters))

        if from_obj is not None:
            outer_subquery = outer_subquery.select_from(from_obj)

        outer_subquery = outer_subquery.alias().lateral()

        if self.verbose:
            compiled_query(outer_subquery, "Outer subquery")

        params = []
        for primary_key in through.model.primary_keys:
            params.append(
                sa.func.JSON_BUILD_OBJECT(
                    str(primary_key),
                    sa.func.JSON_BUILD_ARRAY(through.model.c[primary_key]),
                )
            )

        through_keys = sa.cast(
            sa.func.JSON_BUILD_OBJECT(
                node.relationship.through_tables[0],
                sa.func.JSON_BUILD_ARRAY(*params),
            ),
            sa.dialects.postgresql.JSONB,
        )

        # book author through table
        _keys = sa.func.JSON_AGG(
            sa.cast(
                outer_subquery.c._keys,
                sa.dialects.postgresql.JSONB,
            ).concat(through_keys)
        ).label("_keys")

        left_foreign_keys = foreign_keys[node.name]
        right_foreign_keys = self._get_column_foreign_keys(
            through.columns,
            foreign_keys,
            table=through.table,
            schema=node.schema,
        )

        columns = [
            _keys,
            sa.func.JSON_AGG(outer_subquery.c.anon).label(node.label),
        ]

        foreign_keys = get_foreign_keys(node.parent, through)

        for column in foreign_keys[through.name]:
            columns.append(through.model.c[column])

        inner_subquery = sa.select(columns)

        if self.verbose:
            compiled_query(inner_subquery, "Inner subquery")

        onclause: list = []
        for i in range(len(left_foreign_keys)):
            onclause.append(
                outer_subquery.c[left_foreign_keys[i]]
                == through.model.c[right_foreign_keys[i]]
            )

        if node._filters:
            self.isouter = False

        op = sa.and_
        if node.table == node.parent.table:
            op = sa.or_

        from_obj = through.model.join(
            outer_subquery,
            onclause=op(*onclause),
            isouter=self.isouter,
        )

        node._subquery = inner_subquery.select_from(from_obj)
        if node._filters:
            node._subquery = node._subquery.where(sa.and_(*node._filters))

        node._subquery = node._subquery.group_by(
            *[through.model.c[column] for column in foreign_keys[through.name]]
        )

        if self.verbose:
            compiled_query(node._subquery, "Combined subquery")

        node._subquery = node._subquery.alias()
        if not node.is_root:
            node._subquery = node._subquery.lateral()

    def _non_through(self, node: Node) -> None:  # noqa: C901

        from_obj = None

        for child in node.children:

            onclause: list = []

            foreign_keys = self._get_foreign_keys(node, child)

            foreign_key_columns = self._get_column_foreign_keys(
                child._subquery.columns,
                foreign_keys,
            )

            for i in range(len(foreign_key_columns)):
                onclause.append(
                    child._subquery.c[foreign_key_columns[i]]
                    == node.model.c[foreign_keys[node.name][i]]
                )

            if from_obj is None:
                from_obj = node.model

            if child._filters:
                self.isouter = False

                for _filter in child._filters:
                    if isinstance(_filter, sa.sql.elements.BinaryExpression):
                        for column in _filter._orig:
                            if hasattr(column, "value"):
                                _column = child._subquery.c
                                if column._orig_key in node.table_columns:
                                    _column = node.model.c
                                if hasattr(_column, column._orig_key):
                                    onclause.append(
                                        _column[column._orig_key]
                                        == column.value
                                    )
                    elif isinstance(
                        _filter,
                        sa.sql.elements.BooleanClauseList,
                    ):
                        for clause in _filter.clauses:

                            for column in clause._orig:
                                if hasattr(column, "value"):
                                    _column = child._subquery.c
                                    if column._orig_key in node.table_columns:
                                        _column = node.model.c
                                    if hasattr(_column, column._orig_key):
                                        onclause.append(
                                            _column[column._orig_key]
                                            == column.value
                                        )

            isouter = len(child.parent.children) > 1

            from_obj = from_obj.join(
                child._subquery,
                onclause=sa.and_(*onclause),
                isouter=isouter,
            )

        foreign_keys = get_foreign_keys(node.parent, node)

        foreign_key_columns = self._get_column_foreign_keys(
            node.model.columns,
            foreign_keys,
            table=node.table,
            schema=node.schema,
        )

        params: list = []
        if node.parent.is_root:
            for primary_key in node.primary_keys:
                params.extend(
                    [
                        str(primary_key.name),
                        sa.func.JSON_BUILD_ARRAY(
                            node.model.c[primary_key.name]
                        ),
                    ]
                )
        else:
            for primary_key in node.primary_keys:
                params.extend(
                    [
                        str(primary_key.name),
                        node.model.c[primary_key.name],
                    ]
                )

        if node.relationship.type == ONE_TO_ONE:
            _keys = self._get_child_keys(node, self._json_build_object(params))
        elif node.relationship.type == ONE_TO_MANY:
            _keys = self._get_child_keys(
                node, sa.func.JSON_AGG(self._json_build_object(params))
            )

        columns: List = [_keys]

        if node.relationship.variant == SCALAR:
            # TODO: Raise exception here if the number of columns > 1
            if node.relationship.type == ONE_TO_ONE:
                columns.append(node.model.c[node.columns[0]].label(node.label))
            elif node.relationship.type == ONE_TO_MANY:
                columns.append(
                    sa.func.JSON_AGG(node.model.c[node.columns[0]]).label(
                        node.label
                    )
                )
        elif node.relationship.variant == OBJECT:
            if node.relationship.type == ONE_TO_ONE:
                columns.append(
                    self._json_build_object(node.columns).label(node.label)
                )
            elif node.relationship.type == ONE_TO_MANY:
                columns.append(
                    sa.func.JSON_AGG(
                        self._json_build_object(node.columns)
                    ).label(node.label)
                )

        for column in foreign_key_columns:
            columns.append(node.model.c[column])

        node._subquery = sa.select(columns)

        if from_obj is not None:
            node._subquery = node._subquery.select_from(from_obj)

        parent_foreign_key_columns = self._get_column_foreign_keys(
            node.parent.model.columns,
            foreign_keys,
            table=node.parent.table,
            schema=node.parent.schema,
        )
        where: list = []
        for i in range(len(foreign_key_columns)):
            where.append(
                node.model.c[foreign_key_columns[i]]
                == node.parent.model.c[parent_foreign_key_columns[i]]
            )
        node._subquery = node._subquery.where(sa.and_(*where))

        if node._filters:
            node._subquery = node._subquery.where(sa.and_(*node._filters))

        if node.relationship.type == ONE_TO_MANY:
            node._subquery = node._subquery.group_by(
                *[node.model.c[key] for key in foreign_key_columns]
            )
        node._subquery = node._subquery.alias()

        if not node.is_root:
            node._subquery = node._subquery.lateral()

    def build_queries(self, node: Node) -> None:
        """Build node query."""
        self.from_obj = None

        # 1) add all child columns from one level below
        self._children(node)

        if node.is_root:
            self._root(node)
        else:
            # 2) subquery: these are for children creating their own columns
            if node.relationship.through_tables:
                self._through(node)
            else:
                self._non_through(node)
