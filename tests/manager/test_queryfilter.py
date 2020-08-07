
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Sequence, Column, Integer, String
import sqlalchemy as sa

from ai.backend.manager.models.minilang.queryfilter import Query2sql

test_json = r'''
User(and:{
        eq:{
            name:"tester"
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
                name:"tester"
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

escape_filter = "or:{eq:{name:\"test\'er ♪\"},gt:{age:20}}"
underbar_filter = "eq:{full_name:\"tester1\"}"

Base = declarative_base()
Session = sessionmaker()
engine = create_engine('sqlite:///:memory:', echo=True)
Session.configure(bind=engine)
conn = engine.connect()
q2s = Query2sql()


class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, Sequence('user_id_seq'), primary_key=True)
    name = Column(String(50))
    full_name = Column(String(50))
    age = Column(Integer)

    def __init__(self, name, full_name, age):
        self.name = name
        self.full_name = full_name
        self.age = age

    def __repr__(self):
        return "<User('%s', '%s', '%s')>" % (self.name, self.full_name, self.age)


Base.metadata.create_all(engine)
session = Session()

session.add_all([
    User('tester', 'tester1', 30),
    User('test\'er', 'tester2', 30),
    User('test\'er ♪', 'tester3', 20)])

session.commit()


def test_tree2table():
    tree = q2s.query2tree(test_json)
    table = q2s.tree2table(tree)
    assert table == 'User'


def test_tree2where():
    tree = q2s.query2tree(test_json)
    where_clause = q2s.tree2where(tree)
    assert where_clause == "name = \"tester\" AND age > 20"


def test_tree2select():
    tree = q2s.query2tree(test_json)
    table = q2s.tree2table(tree)
    assert table == "User"


def test_deep_tree2table():
    tree = q2s.query2tree(test_deep_json)
    select_clause = q2s.tree2select(tree)
    assert select_clause == "name, age"


def test_deep_tree2where():
    tree = q2s.query2tree(test_deep_json)
    where_clause = q2s.tree2where(tree)
    assert where_clause == "(name = \"tester\" OR name = \"lablup\") AND age > 20"


def test_deep_tree2select():
    tree = q2s.query2tree(test_deep_json)
    select_clause = q2s.tree2select(tree)
    assert select_clause == "name, age"


def test_escape_filter():
    tree = q2s.query2tree(escape_filter)
    where_clause = q2s.tree2where(tree)
    assert where_clause == "name = \"test\'er ♪\" OR age > 20"


def test_chaining():
    sa_query = sa.select([User.name, User.age]).select_from(User)
    sa_query = q2s.sa_chaining(sa_query, escape_filter)
    ret = list(conn.execute(sa_query))
    test_ret = [("tester", 30), ("test\'er", 30), ("test\'er ♪", 20)]
    assert test_ret == ret


def test_underbar_filter():
    tree = q2s.query2tree(underbar_filter)
    where_clause = q2s.tree2where(tree)
    assert where_clause == "full_name = \"tester1\""
