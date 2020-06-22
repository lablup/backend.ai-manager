import logging

import click
import graphene

from ai.backend.common.logging import BraceStyleAdapter

from ..models.gql import Queries, Mutations

log = BraceStyleAdapter(logging.getLogger(__name__))


@click.group()
def cli(args):
    pass


@cli.command()
@click.pass_obj
def show(cli_ctx):
    with cli_ctx.logger:
        schema = graphene.Schema(
            query=Queries,
            mutation=Mutations,
            auto_camelcase=False)
        log.info('======== GraphQL API Schema ========')
        print(str(schema))
