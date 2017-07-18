import asyncio
import enum
import logging
import uuid

import asyncpgsa as pg
import sqlalchemy as sa
from sqlalchemy.types import TypeDecorator, CHAR
from sqlalchemy.dialects.postgresql import UUID

from .config import load_config, init_logger

log = logging.getLogger('sorna.gateway.models')
metadata = sa.MetaData()

test_user_email = 'testion@sorna.io'


class CurrencyTypes(enum.Enum):
    KRW = 'KRW'
    USD = 'USD'


class GUID(TypeDecorator):
    '''
    Platform-independent GUID type.
    Uses PostgreSQL's UUID type, otherwise uses CHAR(16) storing as raw bytes.
    '''
    impl = CHAR

    def load_dialect_impl(self, dialect):
        if dialect.name == 'postgresql':
            return dialect.type_descriptor(UUID())
        else:
            return dialect.type_descriptor(CHAR(16))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == 'postgresql':
            return str(value)
        else:
            if isinstance(value, uuid.UUID):
                return value.bytes
            else:
                return uuid.UUID(value).bytes

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        else:
            return uuid.UUID(value)


def IDColumn(name='id'):
    return sa.Column('id', GUID, primary_key=True,
                     default=uuid.uuid4,
                     server_default=sa.text("uuid_generate_v4()"))


KeyPair = sa.Table(
    'sorna_cloud_api_keypair', metadata,
    sa.Column('user_id', sa.Integer()),  # foreign key
    sa.Column('access_key', sa.String(length=20), primary_key=True),
    sa.Column('secret_key', sa.String(length=40)),
    sa.Column('is_active', sa.Boolean),
    sa.Column('billing_plan', sa.String, nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
    sa.Column('last_used', sa.DateTime(timezone=True), nullable=True),
    sa.Column('concurrency_limit', sa.Integer),
    sa.Column('concurrency_used', sa.Integer),
    sa.Column('rate_limit', sa.Integer),
    sa.Column('num_queries', sa.Integer, server_default='0'),
    # NOTE: API rate-limiting is done using Redis, not DB.
)

Usage = sa.Table(
    'sorna_cloud_api_usage', metadata,
    #IDColumn('id'),
    sa.Column('access_key_id', sa.ForeignKey('keypairs.access_key')),
    sa.Column('kernel_type', sa.String),
    sa.Column('kernel_id', sa.String),
    sa.Column('started_at', sa.DateTime(timezone=True)),
    sa.Column('terminated_at', sa.DateTime(timezone=True)),
    sa.Column('cpu_used', sa.Integer, server_default='0'),
    sa.Column('mem_used', sa.Integer, server_default='0'),
    sa.Column('io_used', sa.Integer, server_default='0'),
    sa.Column('net_used', sa.Integer, server_default='0'),
)


def mock_engine():
    '''
    Creates a pair of io.StringIO buffer object and a mock engine that does NOT
    execute SQL queries but only dumps the compiled statements into the buffer
    object.
    '''
    from sqlalchemy import create_engine as ce
    from io import StringIO
    buf = StringIO()

    def dump(sql, *multiparams, **params):
        buf.write(str(sql.compile(dialect=engine.dialect)) + ';\n')

    engine = ce('postgresql://', echo=True, strategy='mock', executor=dump)
    return buf, engine


def generate_sql(sa_callable):
    '''
    Generates a compiled SQL statement as a string from SQLAlchemy methods that
    accepts engine as the first function argument.
    '''
    buf, engine = mock_engine()
    sa_callable(engine)
    return buf.getvalue()


if __name__ == '__main__':

    def model_args(parser):
        parser.add('--drop-tables', action='store_true', default=False,
                   help='Drops all tables.')
        parser.add('--create-tables', action='store_true', default=False,
                   help='Creates all tables. If anyone already exists, it will raise an error.')
        parser.add('--populate-fixtures', action='store_true', default=False,
                   help='Populates initial fixture data.')

    config = load_config(extra_args_func=model_args)
    init_logger(config)

    async def drop_tables(config, pool):
        async with pool.acquire() as conn:
            log.warning('Dropping tables... (all data is lost!)')
            await conn.execute(generate_sql(metadata.drop_all))

    async def create_tables(config, pool):
        async with pool.acquire() as conn:
            log.info('Creating tables...')
            # Load an extension to use "uuid_generate_v4()" SQL function
            await conn.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')
            await conn.execute(generate_sql(metadata.create_all))

    async def populate_fixtures(config, pool):
        log.info('Populating fixtures...')
        async with pool.acquire() as conn:
            example_akey = 'AKIAIOSFODNN7EXAMPLE'
            example_skey = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
            query = (sa.select([sa.func.count(KeyPair.c.access_key)])
                       .select_from(KeyPair)
                       .where(KeyPair.c.access_key == example_akey))
            count = await conn.fetchval(query)
            if count == 0:  # only when not exists
                log.info('Creating the default keypair for the test user')
                await conn.execute(KeyPair.insert().values(**{
                    'user_id': 2,  # 1: anonymouse user, 2: default super user
                    'access_key': example_akey,
                    'secret_key': example_skey,
                    'is_active': True,
                    'billing_plan': 'free',
                    'num_queries': 0,
                    'concurrency_limit': 30,
                    'concurrency_used': 0,
                    'rate_limit': 1000,
                    # Sample free tier: 500 launches per day x 30 days per month
                    # 'remaining_cpu': 180000 * 500 * 30,   # msec (180 sec per launch)
                    # 'remaining_mem': 1048576 * 500 * 30,  # KBytes (1GB per launch)
                    # 'remaining_io': 102400 * 500 * 30,    # KBytes (100MB per launch)
                    # 'remaining_net': 102400 * 500 * 30,   # KBytes (100MB per launch)
                }))

    async def init_db(config):
        async with pg.create_pool(
            host=str(config.db_addr[0]),
            port=config.db_addr[1],
            user=config.db_user,
            password=config.db_password,
            database=config.db_name,
            min_size=1, max_size=3,
        ) as pool:
            if config.drop_tables:
                await drop_tables(config, pool)
            if config.create_tables:
                await create_tables(config, pool)
            if config.populate_fixtures:
                await populate_fixtures(config, pool)

    loop = asyncio.get_event_loop()
    log.info('NOTICE: If you see SQL errors, you may need to drop and recreate tables.')
    try:
        loop.run_until_complete(init_db(config))
    finally:
        log.info('Done.')
        loop.close()
