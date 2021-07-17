from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, Column, Sequence, Integer, String
import sqlalchemy as sa
import pytest
from ai.backend.manager.models.minilang.queryfilter import QueryFilterParser


@pytest.fixture
def virtual_user_db():
    engine = sa.engine.create_engine('sqlite:///:memory:', echo=False)
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
    with engine.connect() as conn:
        conn.execute(users.insert(), [
            {'name': 'tester', 'full_name': 'tester1', 'age': 30},
            {'name': 'test\"er', 'full_name': 'tester2', 'age': 40},
            {'name': 'test\'er', 'full_name': 'tester3', 'age': 50},
            {'name': 'tester ♪', 'full_name': 'tester4', 'age': 20}
        ])
        yield conn, users
    engine.dispose()


def test_select_queries(virtual_user_db) -> None:
    conn, users = virtual_user_db
    parser = QueryFilterParser()

    sa_query = parser.append_filter(
        sa.select([users.c.name, users.c.age]).select_from(users),
        "full_name == \"tester1\"",
    )
    actual_ret = list(conn.execute(sa_query))
    test_ret = [("tester", 30)]
    assert test_ret == actual_ret

    sa_query = parser.append_filter(
        sa.select([users.c.name, users.c.age]).select_from(users),
        "(full_name == \"tester1\")",
    )
    actual_ret = list(conn.execute(sa_query))
    test_ret = [("tester", 30)]
    assert test_ret == actual_ret

    sa_query = parser.append_filter(
        sa.select([users.c.name, users.c.age]).select_from(users),
        "full_name == \"tester1\" & age == 20",
    )
    actual_ret = list(conn.execute(sa_query))
    test_ret = []
    assert test_ret == actual_ret

    sa_query = parser.append_filter(
        sa.select([users.c.name, users.c.age]).select_from(users),
        "(full_name == \"tester1\") & (age == 20)",
    )
    actual_ret = list(conn.execute(sa_query))
    test_ret = []
    assert test_ret == actual_ret

    sa_query = parser.append_filter(
        sa.select([users.c.name, users.c.age]).select_from(users),
        "(full_name == \"tester1\") | (age == 20)",
    )
    actual_ret = list(conn.execute(sa_query))
    test_ret = [("tester", 30), ("tester ♪", 20)]
    assert test_ret == actual_ret

    sa_query = parser.append_filter(
        sa.select([users.c.name, users.c.age]).select_from(users),
        "(name contains \"test\") & (age > 30)",
    )
    actual_ret = list(conn.execute(sa_query))
    test_ret = [("test\"er", 40), ("test\'er", 50)]
    assert test_ret == actual_ret

    # invalid syntax
    with pytest.raises(ValueError):
        parser.append_filter(
            sa.select([users.c.name, users.c.age]).select_from(users),
            "!!!",
        )

    # non-existent column
    with pytest.raises(ValueError):
        parser.append_filter(
            sa.select([users.c.name, users.c.age]).select_from(users),
            "xyz == 123",
        )


def test_modification_queries(virtual_user_db) -> None:
    conn, users = virtual_user_db
    parser = QueryFilterParser()

    sa_query = parser.append_filter(
        sa.update(users).values({'name': 'hello'}),
        "full_name == \"tester1\"",
    )
    result = conn.execute(sa_query)
    assert result.rowcount == 1

    sa_query = parser.append_filter(
        sa.delete(users),
        "full_name like \"tester%\"",
    )
    result = conn.execute(sa_query)
    assert result.rowcount == 4


def test_fieldspec(virtual_user_db) -> None:
    conn, users = virtual_user_db
    parser = QueryFilterParser({
        "n1": ("name", None),
        "n2": ("full_name", lambda s: s.lower()),
    })

    sa_query = parser.append_filter(
        sa.select([users.c.name, users.c.age]).select_from(users),
        "n1 == \"tester\"",
    )
    actual_ret = list(conn.execute(sa_query))
    test_ret = [("tester", 30)]
    assert test_ret == actual_ret

    sa_query = parser.append_filter(
        sa.select([users.c.name, users.c.age]).select_from(users),
        "n2 == \"TESTER1\"",
    )
    actual_ret = list(conn.execute(sa_query))
    test_ret = [("tester", 30)]
    assert test_ret == actual_ret

    # non-existent column in fieldspec
    with pytest.raises(ValueError):
        parser.append_filter(
            sa.select([users.c.name, users.c.age]).select_from(users),
            "full_name == \"TESTER1\"",
        )
