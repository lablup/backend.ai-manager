from lark import Lark
import lark
import sqlalchemy as sa


class Query2sql:
    __JSON_GRAMMER = r"""
        ?start: value
        ?value: query
              | object
              | array
              | filter
              | string
              | pair
              | word
              | number
              | "true"             -> true
              | "false"            -> false
              | "null"             -> null
        query  : [word | string] "(" filter ")" object
        array  : "[" [value ("," value)*] "]"
        object : "{" [value ("," value)*] "}"
        filter : [word | string] ":" object
        pair   : [word | string] ":" [word | string | number]
        word   : WORD
        string : ESCAPED_STRING
        number : SIGNED_NUMBER
        %import common.WORD
        %import common.ESCAPED_STRING
        %import common.SIGNED_NUMBER
        %import common.WS
        %ignore WS
    """

    def __init__(self):
        self._FILTERS = {
            'and': 'AND',
            'or': 'OR',
            'gt': '>',
            'gte': '>=',
            'lt': '<',
            'lte': '<=',
            'eq': '=',
            'ne': '!='
        }
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

    def tree_partition(self, tree, ret_table=True, ret_filter=True, ret_select=True):
        ret = []
        if tree.data == 'object':
            tree = tree.children
        if ret_table:
            table = tree.children[0].children
            table = self._token2str(table)
            ret.append(table)
        if ret_filter:
            filter = tree.children[1]
            ret.append(filter)
        if ret_select:
            select = tree.children[2]
            ret.append(select)
        if len(ret) == 1:
            return ret[0]
        return ret

    def tree2table(self, tree):
        table = self.tree_partition(
            tree, ret_table=True, ret_filter=False, ret_select=False)
        return table

    def tree2where(self, tree):
        filter_stack = []
        count_stack = []
        bracket_stack = []
        where = ""
        if tree.data != "filter":
            tree = self.tree_partition(
                tree, ret_table=False, ret_filter=True, ret_select=False)
        tree = list(tree.iter_subtrees_topdown())
        for i in range(len(tree)):
            children = tree[i].children
            if type(children[0]) == lark.lexer.Token:
                token = self._token2str(children)
                if token in self._FILTERS:
                    filter_stack.append(token)
                    if token == 'and' or token == 'or':
                        where += "("
                        count_stack.append(0)
                    elif count_stack:
                        count_stack[-1] += 1
                        while count_stack:
                            if count_stack[-1] == 2:
                                bracket_stack.append(") ")
                                count_stack.pop()
                                if count_stack:
                                    count_stack[-1] += 1
                            else:
                                break
                else:
                    where += token + " "
                    if tree[i - 1].data != "pair":
                        while bracket_stack:
                            where = where[:-1]
                            where += bracket_stack.pop()
                    if filter_stack:
                        where += self._FILTERS[filter_stack.pop()] + " "
        if where[0] == "(" and where[-2] == ")":
            where = where[1:-1]
        return where[:-1]

    def tree2select(self, tree):
        if tree.data != 'object' or tree.children[0].data == 'query':
            tree = self.tree_partition(
                tree, ret_table=False, ret_filter=False, ret_select=True)
        tree = list(tree.iter_subtrees_topdown())
        select = ""
        for i in range(len(tree)):
            children = tree[i].children
            if type(children[0]) == lark.lexer.Token:
                token = self._token2str(children)
                select += token + ", "
        return select[:-2]

    def sa_chaising(self, sa_query, tree=None, table=None, select_clause=None, where_clause=None):
        if tree is not None:
            table = self.tree2table(tree)
            select_clause = self.tree2select(tree)
            where_clause = self.tree2where(tree)
        assert table is not None
        if select_clause is not None:
            sa_query = sa_query.select(
                [text(select_clause)]).select_from(text(table))
        else:
            sa_query = sa_query.select(text(table))
        if where_clause is not None:
            sa_query = sa_query.where(text(where_clause))
        return sa_query
