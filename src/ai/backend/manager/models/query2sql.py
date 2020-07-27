from lark import Lark
import lark
import sqlalchemy as sa
from sqlalchemy.sql import text


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
            data = tree[i].data
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
                    if tree[i-1].data != "pair":
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
            data = tree[i].data
            children = tree[i].children
            if type(children[0]) == lark.lexer.Token:
                token = self._token2str(children)
                select += token + ", "
        return select[:-2]

    def sa_chaising(self, sa_query, tree=None, table=None, select_clause=None, where_clause=None):
        if tree != None:
            table = self.tree2table(tree)
            select_clause = self.tree2select(tree)
            where_clause = self.tree2where(tree)
        assert table != None
        if select_clause != None:
            sa_query = sa_query.select(
                [text(select_clause)]).select_from(text(table))
        else:
            sa_query = sa_query.select(text(table))
        if where_clause != None:
            sa_query = sa_query.where(text(where_clause))
        return sa_query


if __name__ == "__main__":
    test_json = r'''
    User(and:{
            eq:{
                name:"dongyun"
            },
            gt:{
                age:20
            }
        }) {
        name, 
        age 
    }
    '''
    q2s = Query2sql()
    tree = q2s.query2tree(test_json)
    table, filter, select = q2s.tree_partition(tree)
    print("tree_partition return table:\n   ", table, type(table))
    table = q2s.tree2table(tree)
    print("tree2table return:\n   ", table, type(table))
    print("tree_partition return filter:\n   ", filter)
    print("tree_partition return select:\n   ", select)
    where_clause = q2s.tree2where(tree)
    select_clause = q2s.tree2select(tree)
    print("tree2select return:\n   ", select_clause, type(select_clause))
    print("tree2swhere return:\n   ", where_clause, type(where_clause))
    print(sa.__version__)
