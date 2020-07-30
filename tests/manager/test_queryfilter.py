from ai.backend.manager.models.minilang.queryfilter import Query2sql

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

test_deep_json = r'''
User(and:{
        or:{
            eq:{
                name:"dongyun"
            },
            eq:{
                name:"lablup"
            }
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


def test_tree2table():
    tree = q2s.query2tree(test_json)
    table = q2s.tree2table(tree)
    assert table == 'User'


def test_tree2where():
    tree = q2s.query2tree(test_json)
    where_clause = q2s.tree2where(tree)
    assert where_clause == 'name = "dongyun" AND age > 20'


def test_tree2select():
    tree = q2s.query2tree(test_json)
    table = q2s.tree2table(tree)
    assert table == 'User'


def test_deep_tree2table():
    tree = q2s.query2tree(test_deep_json)
    select_clause = q2s.tree2select(tree)
    assert select_clause == 'name, age'


def test_deep_tree2where():
    tree = q2s.query2tree(test_deep_json)
    where_clause = q2s.tree2where(tree)
    assert where_clause == '(name = "dongyun" OR name = "lablup") AND age > 20'


def test_deep_tree2select():
    tree = q2s.query2tree(test_deep_json)
    select_clause = q2s.tree2select(tree)
    assert select_clause == 'name, age'
