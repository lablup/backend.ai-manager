from lark import Lark
import lark
from .grammer_checker import checkBracket, checkQuotes, checkEscapeString
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
        query  : [word|string] "(" filter ")" object
        array  : "[" [value ("," value)*] "]"
        object : "{" [value ("," value)*] "}"
        filter : [word|string] ":" object
        pair   : [word|string] ":" [word|string|number]
        word   : CNAME
        string : ESCAPED_STRING
        number : SIGNED_NUMBER
        %import common.CNAME
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
        if not checkBracket(query):
            raise ValueError(r'''
            Check the brackets in the query string.
            ''')
        if not checkQuotes(query):
            raise ValueError(r'''
            Check the quotes in the value string.
            If you want to write quotes in the value string, use ' instead of ".
            ''')
        if not checkEscapeString(query):
            raise ValueError(r'''
            Be careful with the use of Escape string.
            Escape string can only be used within value string.
            Double Quotes(") can only be used to represent value string type.
            If you want to write quotes in the value string, use ' instead of ".
            ''')
        tree = self._JSON_PARSER.parse(query)
        return tree

    def _check_type(self, tree, type=""):
        if type == 'word':
            if type(tree) == str:
                return True
        elif tree.data == type:
            return True
        return False

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

    def tree_validation(self, tree, validation):
        tree = list(tree.iter_subtrees_topdown())
        for i in range(len(tree)):
            data = tree[i].data
            children = tree[i].children
            if data == 'word':
                token = self._token2str(children)
                if token in self._FILTERS:
                    continue
                if token not in validation:
                    return False
        return True

    def sa_chaining(self, sa_query, query_arg, type="filter", validation=None):
        tree = self.query2tree(query_arg)
        if not self._check_type(tree, type):
            raise ValueError(r'''
            The form of the query_arg is not a {}
            '''.format(type))
        where_clause = self.tree2where(tree)
        if validation:
            if not self.tree_validation(tree, validation):
                raise ValueError(r'''
                Not exist column name in table
                ''')
        sa_query = sa_query.where(text(where_clause))
        return sa_query
