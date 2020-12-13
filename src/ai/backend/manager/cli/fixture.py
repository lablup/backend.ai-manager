from __future__ import annotations

import logging
import json
from pathlib import Path
from typing import TYPE_CHECKING

import click
import sqlalchemy as sa

from ai.backend.common.logging import BraceStyleAdapter

from ..models.base import populate_fixture
if TYPE_CHECKING:
    from .__main__ import CLIContext

log = BraceStyleAdapter(logging.getLogger(__name__))


@click.group()
def cli():
    pass


@cli.command()
@click.argument('fixture_path', type=Path)
@click.pass_obj
def populate(cli_ctx: CLIContext, fixture_path) -> None:
    """Populate fixtures."""
    with cli_ctx.logger:
        log.info("populating fixture '{0}'", fixture_path)
        try:
            fixture = json.loads(fixture_path.read_text(encoding='utf8'))
        except AttributeError:
            log.error('No such fixture.')
            return

        engine = sa.create_engine(
            f"postgres://{cli_ctx.local_config['db']['user']}:{cli_ctx.local_config['db']['password']}"
            f"@{cli_ctx.local_config['db']['addr']}/{cli_ctx.local_config['db']['name']}")
        conn = engine.connect()
        populate_fixture(conn, fixture)
        conn.close()


@cli.command()
@click.pass_obj
def list(cli_ctx: CLIContext) -> None:
    """List all available fixtures."""
    with cli_ctx.logger:
        log.warning('This command is deprecated.')
