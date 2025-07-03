"""PGSync QueryBuilder."""

import threading
import typing as t
from collections import defaultdict

import sqlalchemy as sa

from .base import compiled_query, TupleIdentifierType
from .constants import OBJECT, ONE_TO_MANY, ONE_TO_ONE, SCALAR
from .exc import ForeignKeyError
from .node import Node


class QueryBuilder(threading.local):
    """Query builder."""

    def __init__(self, verbose: bool = False):
        """Query builder constructor."""
        self.verbose: bool = verbose
        self.isouter: bool = True
        self._cache: dict = {}

    def _eval_expression(
        self, expression: sa.sql.elements.BinaryExpression
    ) -> sa.sql.elements.BinaryExpression:
        if isinstance(
            expression.left.type, sa.dialects.postgresql.UUID
        ) or isinstance(expression.right.type, sa.dialects.postgresql.UUID):
            if not isinstance(
                expression.left.type, sa.dialects.postgresql.UUID
            ) or not isinstance(
                expression.right.type, sa.dialects.postgresql.UUID
            ):
                # handle UUID typed expressions:
                # psycopg2.errors.UndefinedFunction: operator does not exist: uuid = integer
                return expression.left is None

        return expression

    def _build_filters(
        self, filters: t.Dict[str, t.List[dict]], node: Node
    ) -> t.Optional[sa.sql.elements.BooleanClauseList]:
        """
        Build SQLAlchemy filters.

        NB:
        assumption dictionary is an AND and list is an OR
        filters = {
            'book': [
                {'id': 1, 'uid': '001'},
                {'id': 2, 'uid': '002'},
            ],
            'city': [
                {'id': 1},
                {'id': 2},
            ],
        }
        """
        if filters is not None:
            if filters.get(node.table):
                clause: t.List = []
                for values in filters.get(node.table):
                    where: t.List = []
                    for column, value in values.items():
                        where.append(
                            self._eval_expression(
                                node.model.c[column] == value
                            )
                        )
                    # and clause is applied for composite primary keys
                    clause.append(sa.and_(*where))
                return sa.or_(*clause)

    def _json_build_object(
        self, columns: t.List, chunk_size: int = 100
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
        expression: sa.sql.elements.BinaryExpression = None
        while i < len(columns):
            chunk: t.List = columns[i : i + chunk_size]
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

    def get_foreign_keys_through_model(self, node_a: Node, node_b: Node) -> dict:
        if (node_a, node_b) not in self._cache:
            fkeys: dict = defaultdict(list)
            if node_a.model.foreign_keys:
                for key in node_a.model.original.foreign_keys:
                    if key._table_key() == str(node_b.model.original):
                        fkeys[
                            f"{key.parent.table.schema}."
                            f"{key.parent.table.name}"
                        ].append(str(key.parent.name))
                        fkeys[
                            f"{key.column.table.schema}."
                            f"{key.column.table.name}"
                        ].append(str(key.column.name))
            if not fkeys:
                if node_b.model.original.foreign_keys:
                    for key in node_b.model.original.foreign_keys:
                        if key._table_key() == str(node_a.model.original):
                            fkeys[
                                f"{key.parent.table.schema}."
                                f"{key.parent.table.name}"
                            ].append(str(key.parent.name))
                            fkeys[
                                f"{key.column.table.schema}."
                                f"{key.column.table.name}"
                            ].append(str(key.column.name))
            if not fkeys:
                raise ForeignKeyError(
                    f"No foreign key relationship between "
                    f"{node_a.model.original} and {node_b.model.original}"
                )

            foreign_keys: dict = {}
            for table, columns in fkeys.items():
                foreign_keys[table] = columns

            self._cache[(node_a, node_b)] = foreign_keys

        return self._cache[(node_a, node_b)]

    # this is for handling non-through tables
    def get_foreign_keys(self, node_a: Node, node_b: Node) -> dict:
        if (node_a, node_b) not in self._cache:
            foreign_keys: dict = {}
            # if either offers a foreign_key via relationship, use it!
            if (
                node_a.relationship.foreign_key.parent
                or node_b.relationship.foreign_key.parent
            ):
                if node_a.relationship.foreign_key.parent:
                    foreign_keys[node_a.parent.name] = sorted(
                        node_a.relationship.foreign_key.parent
                    )
                    foreign_keys[node_a.name] = sorted(
                        node_a.relationship.foreign_key.child
                    )
                if node_b.relationship.foreign_key.parent:
                    foreign_keys[node_b.parent.name] = sorted(
                        node_b.relationship.foreign_key.parent
                    )
                    foreign_keys[node_b.name] = sorted(
                        node_b.relationship.foreign_key.child
                    )
                self._cache[(node_a, node_b)] = foreign_keys
            else:
                self.get_foreign_keys_through_model(node_a, node_b)

        return self._cache[(node_a, node_b)]

    def _get_foreign_keys(self, node_a: Node, node_b: Node) -> dict:
        """This is for handling through nodes."""
        if (node_a, node_b) not in self._cache:
            if node_a.relationship.throughs or node_b.relationship.throughs:
                if node_a.relationship.throughs:
                    through: Node = node_a.relationship.throughs[0]

                if node_b.relationship.throughs:
                    through: Node = node_b.relationship.throughs[0]
                    node_a, node_b = node_b, node_a

                foreign_keys: dict = self.get_foreign_keys(node_a, through)
                for key, values in self.get_foreign_keys(
                    through, node_b
                ).items():
                    if key in foreign_keys:
                        for value in values:
                            if value not in foreign_keys[key]:
                                foreign_keys[key].append(value)
                        continue
                    foreign_keys[key] = values
            else:
                foreign_keys = self.get_foreign_keys(node_a, node_b)

            self._cache[(node_a, node_b)] = foreign_keys

        return self._cache[(node_a, node_b)]

    def _get_column_foreign_keys(
        self,
        columns: t.List[str],
        foreign_keys: dict,
        table: str = None,
        schema: str = None,
    ) -> list:
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
            column_names: t.List[str] = [column.name for column in columns]
        except AttributeError:
            column_names: t.List[str] = [column for column in columns]

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

    def _get_child_keys(
        self, node: Node, params: dict
    ) -> sa.sql.elements.Label:
        row = sa.cast(
            sa.func.JSON_BUILD_OBJECT(
                node.table,
                params,
            ),
            sa.dialects.postgresql.JSONB,
        )
        for child in node.children:
            if (
                not child.parent.relationship.throughs
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

    def _root(
        self,
        node: Node,
        txmin: t.Optional[int] = None,
        txmax: t.Optional[int] = None,
        ctid: t.Optional[dict] = None,
    ) -> None:
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
        node._subquery = sa.select(*columns)

        if self.from_obj is not None:
            node._subquery = node._subquery.select_from(self.from_obj)

        if ctid is not None:
            subquery = []
            for page, rows in ctid.items():
                subquery.append(
                    sa.select(
                        *[
                            sa.cast(
                                sa.literal_column(f"'({page},'")
                                .concat(sa.column("s"))
                                .concat(")"),
                                TupleIdentifierType,
                            )
                        ]
                    ).select_from(
                        sa.sql.Values(
                            sa.column("s"),
                        )
                        .data([(row,) for row in rows])
                        .alias("s")
                    )
                )
            if subquery:
                node._filters.append(
                    sa.or_(
                        *[
                            node.model.c.ctid
                            == sa.any_(sa.func.ARRAY(q.scalar_subquery()))
                            for q in subquery
                        ]
                    )
                )

        if txmin:
            node._filters.append(
                sa.cast(
                    sa.cast(
                        node.model.c.xmin,
                        sa.Text,
                    ),
                    sa.BigInteger,
                )
                >= txmin
            )
        if txmax:
            node._filters.append(
                sa.cast(
                    sa.cast(
                        node.model.c.xmin,
                        sa.Text,
                    ),
                    sa.BigInteger,
                )
                < txmax
            )

        # NB: only apply filters to the root node
        if node._filters:
            node._subquery = node._subquery.where(sa.and_(*node._filters))
        node._subquery = node._subquery.alias()

        if not node.is_root:
            node._subquery = node._subquery.lateral()

    def _children(self, node: Node) -> None:
        for child in node.children:
            onclause: t.List = []

            if child.relationship.throughs:
                child.parent.columns.extend(
                    [
                        child.label,
                        child._subquery.c[child.label],
                    ]
                )

                through: Node = child.relationship.throughs[0]
                foreign_keys: dict = self.get_foreign_keys(
                    child.parent, through
                )

                left_foreign_keys: list = self._get_column_foreign_keys(
                    child._subquery.columns,
                    foreign_keys,
                    table=through.name,
                    schema=child.schema,
                )
                right_foreign_keys: list = self._get_column_foreign_keys(
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

                foreign_keys: dict = self.get_foreign_keys(node, child)
                left_foreign_keys: list = self._get_column_foreign_keys(
                    child._subquery.columns,
                    foreign_keys,
                )

                if left_foreign_keys == child.table:
                    right_foreign_keys = left_foreign_keys
                else:
                    right_foreign_keys: list = self._get_column_foreign_keys(
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
        through: Node = node.relationship.throughs[0]
        foreign_keys: dict = self.get_foreign_keys_through_model(node, through)

        for key, values in self.get_foreign_keys_through_model(through, node.parent).items():
            if key in foreign_keys:
                for value in values:
                    if value not in foreign_keys[key]:
                        foreign_keys[key].append(value)
                continue
            foreign_keys[key] = values

        foreign_key_columns: list = self._get_column_foreign_keys(
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

        _keys: sa.sql.elements.Label = self._get_child_keys(
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

            if child.relationship.throughs:
                child_through: Node = child.relationship.throughs[0]
                child_foreign_keys: dict = self.get_foreign_keys(
                    child, child_through
                )
                for key, values in self.get_foreign_keys(
                    child_through,
                    child.parent,
                ).items():
                    if key in child_foreign_keys:
                        for value in values:
                            if value not in child_foreign_keys[key]:
                                child_foreign_keys[key].append(value)
                        continue
                    child_foreign_keys[key] = values

                left_foreign_keys: list = self._get_column_foreign_keys(
                    child._subquery.columns,
                    child_foreign_keys,
                    table=child_through.table,
                    schema=child.schema,
                )

            else:
                child_foreign_keys: dict = self.get_foreign_keys(
                    child.parent, child
                )
                left_foreign_keys: list = self._get_column_foreign_keys(
                    child._subquery.columns,
                    child_foreign_keys,
                )

            right_foreign_keys: list = child_foreign_keys[child.parent.name]

            for i in range(len(left_foreign_keys)):
                onclause.append(
                    child._subquery.c[left_foreign_keys[i]]
                    == child.parent.model.c[right_foreign_keys[i]]
                )

            if from_obj is None:
                from_obj = node.model

            if child._filters:

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

        outer_subquery = sa.select(*columns)

        parent_foreign_key_columns: list = self._get_column_foreign_keys(
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
                node.relationship.throughs[0].table,
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
        right_foreign_keys: list = self._get_column_foreign_keys(
            through.columns,
            foreign_keys,
            table=through.table,
            schema=node.schema,
        )

        columns = [
            _keys,
            sa.func.JSON_AGG(outer_subquery.c.anon).label(node.label),
        ]

        foreign_keys: dict = self.get_foreign_keys_through_model(node.parent, through)

        for column in foreign_keys[through.name]:
            columns.append(through.model.c[str(column)])

        inner_subquery = sa.select(*columns)

        if self.verbose:
            compiled_query(inner_subquery, "Inner subquery")

        onclause: list = []
        for i in range(len(left_foreign_keys)):
            onclause.append(
                outer_subquery.c[left_foreign_keys[i]]
                == through.model.c[right_foreign_keys[i]]
            )

        op = sa.and_
        if node.table == node.parent.table:
            op = sa.or_

        from_obj = through.model.join(
            outer_subquery,
            onclause=op(*onclause),
            isouter=self.isouter,
        )

        node._subquery = inner_subquery.select_from(from_obj)

        # NB do not apply filters to the child node as they are applied to the parent
        # if node._filters:
        #     node._subquery = node._subquery.where(sa.and_(*node._filters))

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
            onclause: t.List = []

            foreign_keys: dict = self._get_foreign_keys(node, child)
            table: t.Optional[str] = (
                child.relationship.throughs[0].table
                if child.relationship.throughs
                else None
            )
            foreign_key_columns: list = self._get_column_foreign_keys(
                child._subquery.columns,
                foreign_keys,
                table=table,
                schema=child.schema,
            )

            for i in range(len(foreign_key_columns)):
                onclause.append(
                    child._subquery.c[foreign_key_columns[i]]
                    == node.model.c[foreign_keys[node.name][i]]
                )

            if from_obj is None:
                from_obj = node.model

            if child._filters:

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

            from_obj = from_obj.join(
                child._subquery,
                onclause=sa.and_(*onclause),
                isouter=self.isouter,
            )

        foreign_keys: dict = self.get_foreign_keys(node.parent, node)

        foreign_key_columns: list = self._get_column_foreign_keys(
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

        columns: t.List = [_keys]

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

        node._subquery = sa.select(*columns)

        if from_obj is not None:
            node._subquery = node._subquery.select_from(from_obj)

        parent_foreign_key_columns: list = self._get_column_foreign_keys(
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

        # NB do not apply filters to the child node as they are applied to the parent
        # if node._filters:
        #     node._subquery = node._subquery.where(sa.and_(*node._filters))

        if node.relationship.type == ONE_TO_MANY:
            node._subquery = node._subquery.group_by(
                *[node.model.c[key] for key in foreign_key_columns]
            )
        node._subquery = node._subquery.alias()

        if not node.is_root:
            node._subquery = node._subquery.lateral()

    def build_queries(
        self,
        node: Node,
        filters: t.Optional[dict] = None,
        txmin: t.Optional[int] = None,
        txmax: t.Optional[int] = None,
        ctid: t.Optional[dict] = None,
    ) -> None:
        """Build node query."""
        self.from_obj = None
        _filters = self._build_filters(filters, node)
        if _filters is not None:
            node._filters.append(_filters)

        # 1) add all child columns from one level below
        self._children(node)

        if node.is_root:
            self._root(node, txmin=txmin, txmax=txmax, ctid=ctid)
        else:
            # 2) subquery: these are for children creating their own columns
            if node.relationship.throughs:
                self._through(node)
            else:
                self._non_through(node)
