import asyncio
from datetime import datetime
import logging

import asyncpgsa as pg
import sqlalchemy as sa

from .config import load_config, init_logger
from ..manager.models import keypairs

log = logging.getLogger('sorna.manager.models')

test_user_email = 'testion@sorna.io'


async def populate_fixtures(config, pool):
    log.info('Populating fixtures...')
    async with pool.acquire() as conn:
        example_akey = 'AKIAIOSFODNN7EXAMPLE'
        example_skey = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
        query = (sa.select([sa.func.count(keypairs.c.access_key)])
                   .select_from(keypairs)
                   .where(keypairs.c.access_key == example_akey))
        count = await conn.fetchval(query)
        if count == 0:  # only when not exists
            log.info('Creating the default keypair for the test user')
            await conn.execute(keypairs.insert().values(**{
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
