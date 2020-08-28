from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Sequence, Integer, String
import sqlalchemy as sa
import pytest
from ai.backend.manager.models.minilang.queryfilter import QueryFilterParser

example_filter_with_underbar = "eq:{full_name:\'tester1\'}"
example_filter_with_single_queotes = "eq:{name:\'test\"er\'}"
example_filter_with_double_queotes = "eq:{name:\"test\'er\"}"
example_filter_with_special_character = "and:{eq:{name:\"tester ♪\"}, eq:{name:\'tester ♪\'}}"
example_filter_with_not_exist_column = "eq:{middle_name:\"test\"}"
deep_example_filter = "or:{eq:{full_name:\"tester1\"}, or:{eq:{name:\"tester ♪\"}, gt:{age:20}}}"

parser = QueryFilterParser()


@pytest.fixture(scope="module")
def virtual_user_db():
    engine = create_engine('sqlite:///:memory:', echo=False)
    base = declarative_base()
    metadata = base.metadata
    users = Table(
        'users', metadata,
        Column('id', Integer, Sequence('user_id_seq'), primary_key=True),
        Column('name', String(50)),
        Column('full_name', String(50)),
        Column('age', Integer)
    )
    metadata.create_all(engine)
    conn = engine.connect()
    conn.execute(users.insert(), [
        {'name': 'tester', 'full_name': 'tester1', 'age': 30},
        {'name': 'test\"er', 'full_name': 'tester2', 'age': 30},
        {'name': 'test\'er', 'full_name': 'tester3', 'age': 30},
        {'name': 'tester ♪', 'full_name': 'tester4', 'age': 20}
    ])
    yield conn, users
    conn.close()


def test_example_filter_with_underbar(virtual_user_db):
    conn, users = virtual_user_db
    sa_query = sa.select([users.c.name, users.c.age]).select_from(users)
    sa_query = parser.append_filter(sa_query, example_filter_with_underbar)
    ret = list(conn.execute(sa_query))
    test_ret = [("tester", 30)]
    assert test_ret == ret


def test_example_filter_with_single_queotes(virtual_user_db):
    conn, users = virtual_user_db
    sa_query = sa.select([users.c.name, users.c.age]).select_from(users)
    sa_query = parser.append_filter(sa_query, example_filter_with_single_queotes)
    ret = list(conn.execute(sa_query))
    test_ret = [("test\"er", 30)]
    assert test_ret == ret


def test_example_filter_with_double_queotes(virtual_user_db):
    conn, users = virtual_user_db
    sa_query = sa.select([users.c.name, users.c.age]).select_from(users)
    sa_query = parser.append_filter(sa_query, example_filter_with_double_queotes)
    ret = list(conn.execute(sa_query))
    test_ret = [("test\'er", 30)]
    assert test_ret == ret


def test_example_filter_with_special_character(virtual_user_db):
    conn, users = virtual_user_db
    sa_query = sa.select([users.c.name, users.c.age]).select_from(users)
    sa_query = parser.append_filter(sa_query, example_filter_with_special_character)
    ret = list(conn.execute(sa_query))
    test_ret = [("tester ♪", 20)]
    assert test_ret == ret


def test_example_filter_with_not_exist_column(virtual_user_db):
    _, users = virtual_user_db
    sa_query = sa.select([users.c.name, users.c.age]).select_from(users)
    with pytest.raises(ValueError):
        sa_query = parser.append_filter(sa_query, example_filter_with_not_exist_column)


def test_deep_example_filter(virtual_user_db):
    conn, users = virtual_user_db
    sa_query = sa.select([users.c.name, users.c.age]).select_from(users)
    sa_query = parser.append_filter(sa_query, deep_example_filter)
    ret = list(conn.execute(sa_query))
    test_ret = [("tester", 30), ("test\"er", 30), ("test\'er", 30), ("tester ♪", 20)]
    assert test_ret == ret


def test_modification_queries(virtual_user_db):
    conn, users = virtual_user_db
    sa_query = sa.update(users).values({'name': 'hello'})
    sa_query = parser.append_filter(sa_query, "eq:{full_name:'tester1'}")
    result = conn.execute(sa_query)
    assert result.rowcount == 1
    sa_query = sa.delete(users)
    sa_query = parser.append_filter(sa_query, "eq:{full_name:'tester1'}")
    result = conn.execute(sa_query)
    assert result.rowcount == 1
