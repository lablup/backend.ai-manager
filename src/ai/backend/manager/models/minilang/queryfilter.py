from lark import Lark
import lark
from sqlalchemy.sql import and_, or_, not_


class Query2sql:
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

    def __init__(self):
        self._FILTERS = ['and', 'or', 'not', 'gt', 'gte', 'lt', 'lte', 'eq', 'ne']
        self._JSON_PARSER = Lark(
            Query2sql.__JSON_GRAMMER, parser='lalr', maybe_placeholders=False)

    def query2tree(self, query):
        tree = self._JSON_PARSER.parse(query)
        return tree

    def _token2str(self, token):
        if type(token) == list:
            token = token[0]
        assert type(token) == lark.lexer.Token
        return str(token)

    def tree2where(self, tree, table):
        values_stack = []
        op_stack = []
        cond_stack = []
        tree = list(tree.iter_subtrees_topdown())
        for index, subtree in enumerate(tree):
            data = subtree.data
            children = subtree.children
            if type(children[0]) == lark.lexer.Token:
                token = self._token2str(children)
                if token in self._FILTERS:
                    op_stack.append(token)
                else:
                    if data == "word":
                        if token not in table.c:
                            raise ValueError(r'''
                            Not exist column name in table
                            ''')
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

    def tree_validation(self, tree, table):
        tree = list(tree.iter_subtrees_topdown())
        for index, subtree in enumerate(tree):
            data = subtree.data
            children = subtree.children
            if data == 'word':
                token = self._token2str(children)
                if token in self._FILTERS:
                    continue
                if token not in table.c:
                    return False
        return True

    def sa_chaining(self, sa_query, query_arg, table):
        tree = self.query2tree(query_arg)
        if tree.data != "filter":
            raise ValueError(r'''
            The form of the query_arg is not a filter
            ''')
        where_clause = self.tree2where(tree, table)
        sa_query = sa_query.where(where_clause)
        return sa_query
