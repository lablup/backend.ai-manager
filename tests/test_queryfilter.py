import pytest
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base

from ai.backend.manager.models.minilang.queryfilter import QueryFilterParser


@pytest.fixture
def virtual_user_db():
    engine = sa.engine.create_engine('sqlite:///:memory:', echo=False)
    base = declarative_base()
    metadata = base.metadata
    users = sa.Table(
        'users', metadata,
        sa.Column('id', sa.Integer, sa.Sequence('user_id_seq'), primary_key=True),
        sa.Column('name', sa.String(50)),
        sa.Column('full_name', sa.String(50)),
        sa.Column('age', sa.Integer),
        sa.Column('is_active', sa.Boolean),
        sa.Column('data', sa.Float, nullable=True),
    )
    metadata.create_all(engine)
    with engine.connect() as conn:
        conn.execute(users.insert(), [
            {'name': 'tester', 'full_name': 'tester1', 'age': 30, 'is_active': True, 'data': 10.5},
            {'name': 'test\"er', 'full_name': 'tester2', 'age': 40, 'is_active': True, 'data': None},
            {'name': 'test\'er', 'full_name': 'tester3', 'age': 50, 'is_active': False, 'data': 2.33},
            {'name': 'tester ♪', 'full_name': 'tester4', 'age': 20, 'is_active': False, 'data': None}
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
        "name == \"test'er\"",
    )
    actual_ret = list(conn.execute(sa_query))
    test_ret = [("test'er", 50)]
    assert test_ret == actual_ret

    sa_query = parser.append_filter(
        sa.select([users.c.name, users.c.age]).select_from(users),
        "name == \"test\\\"er\"",
    )
    actual_ret = list(conn.execute(sa_query))
    test_ret = [("test\"er", 40)]
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
        "(name contains \"test\") & (age > 30) & (is_active is true)",
    )
    actual_ret = list(conn.execute(sa_query))
    test_ret = [("test\"er", 40)]
    assert test_ret == actual_ret

    sa_query = parser.append_filter(
        sa.select([users.c.name, users.c.age]).select_from(users),
        "data isnot null"
    )
    actual_ret = list(conn.execute(sa_query))
    test_ret = [("tester", 30), ("test\'er", 50)]
    assert test_ret == actual_ret

    sa_query = parser.append_filter(
        sa.select([users.c.name, users.c.age]).select_from(users),
        "data is null"
    )
    actual_ret = list(conn.execute(sa_query))
    test_ret = [("test\"er", 40), ("tester ♪", 20)]
    assert test_ret == actual_ret

    sa_query = parser.append_filter(
        sa.select([users.c.name, users.c.age]).select_from(users),
        "data < 9.4"
    )
    actual_ret = list(conn.execute(sa_query))
    test_ret = [("test\'er", 50)]  # Note: null values are not matched
    assert test_ret == actual_ret

    # invalid syntax
    with pytest.raises(ValueError):
        parser.append_filter(
            sa.select([users.c.name, users.c.age]).select_from(users),
            "!!!",
        )

    # invalid value type
    # => This case is handled during the actual execution of SQL statements
    #    in the database, not when preparing statements.
    #    So it is the out of scope issue.
    # with pytest.raises(ValueError):
    #     parser.append_filter(
    #         sa.select([users.c.name, users.c.age]).select_from(users),
    #         "full_name == 123",
    #     )

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
