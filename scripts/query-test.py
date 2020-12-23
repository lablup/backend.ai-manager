import code
import configparser

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as psql

from ai.backend.manager.models import (
    agents,
    keypairs,
    kernels,
    users,
)


def main():
    config = configparser.ConfigParser()
    config.read("alembic.ini")
    url = config["alembic"]["sqlalchemy.url"]
    engine = sa.create_engine(url)
    with engine.connect() as connection:
        code.interact(local={
            'sa': sa,
            'conn': connection,
            'psql': psql,
            'agents': agents,
            'keypairs': keypairs,
            'kernels': kernels,
            'users': users,
        })


if __name__ == "__main__":
    main()
