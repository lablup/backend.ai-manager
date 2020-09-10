from typing import (
    Union,
)

from lark import Lark
import lark
import sqlalchemy as sa
from sqlalchemy.sql import and_, or_, not_
from sqlalchemy.sql.elements import ClauseElement


FilterableSQLQuery = Union[sa.sql.Select, sa.sql.Update, sa.sql.Delete]


class QueryFilterParser():
    __JSON_GRAMMER = r"""
        ?start: value
        ?value: object
              | array
              | filter
              | string
              | pair
              | word
              | number
        array  : "[" [value ("," value)*] "]"
        object : "{" [value ("," value)*] "}"
        filter : [word|string] ":" object
        pair   : [word|string] ":" [word|string|number]
        word   : CNAME
        string : ESCAPED_STRING | /'[^']*'/
        number : SIGNED_NUMBER
        %import common.CNAME
        %import common.ESCAPED_STRING
        %import common.SIGNED_NUMBER
        %import common.WS
        %ignore WS
    """

    def __init__(self) -> None:
        self._FILTERS = ['and', 'or', 'not', 'gt', 'gte', 'lt', 'lte', 'eq', 'ne']
        self._JSON_PARSER = Lark(
            self.__JSON_GRAMMER,
            parser='lalr',
            maybe_placeholders=False,
        )

    def _parse_query(self, query: str) -> lark.tree.Tree:
        tree = self._JSON_PARSER.parse(query)
        return tree

    def _ast_to_where_clause(self, tree: lark.tree.Tree, table: sa.Table) -> ClauseElement:
        values_stack = []
        op_stack = []
        cond_stack = []
        for index, subtree in enumerate(tree.iter_subtrees_topdown()):
            data = subtree.data
            children = subtree.children
            if children and isinstance(children[0], lark.lexer.Token):
                token = str(children[0])
                if token in self._FILTERS:
                    op_stack.append(token)
                else:
                    if data == "word":
                        if token not in table.c:
                            raise ValueError("Unknown column name in the table", token)
                        values_stack.append(table.c[token])
                    elif data == "string":
                        values_stack.append(token[1:-1])
                    else:
                        values_stack.append(token)
        while op_stack:
            op = op_stack.pop()
            if op == "not":
                cond = not_(values_stack.pop())
                cond_stack.append(cond)
                continue
            if op in ["and", "or"]:
                value1 = cond_stack.pop()
                value2 = cond_stack.pop()
            else:
                value2 = values_stack.pop()
                value1 = values_stack.pop()
            if op == "gt":
                cond = value1 > value2
            elif op == "gte":
                cond = value1 >= value2
            elif op == "lt":
                cond = value1 < value2
            elif op == "lte":
                cond = value1 <= value2
            elif op == "eq":
                cond = value1 == value2
            elif op == "ne":
                cond = value1 != value2
            elif op == "and":
                cond = and_(value1, value2)
            elif op == "or":
                cond = or_(value1, value2)
            cond_stack.append(cond)
        return cond_stack.pop()

    def append_filter(
        self,
        sa_query: FilterableSQLQuery,
        filter_expr: str,
    ) -> FilterableSQLQuery:
        """
        Parse the given filter expression and build the where clause based on the first target table from
        the given SQLAlchemy query object.
        """
        if isinstance(sa_query, sa.sql.Select):
            table = sa_query.selectable.locate_all_froms()[0]
        elif isinstance(sa_query, sa.sql.Delete):
            table = sa_query.table
        elif isinstance(sa_query, sa.sql.Update):
            table = sa_query.table
        else:
            raise ValueError('Unsupported SQLAlchemy query object type')
        ast = self._parse_query(filter_expr)
        if ast.data != "filter":
            raise ValueError("The form of the query_arg is not a filter")
        where_clause = self._ast_to_where_clause(ast, table)
        return sa_query.where(where_clause)
