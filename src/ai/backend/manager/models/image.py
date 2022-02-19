from __future__ import annotations
from decimal import Decimal
import functools

import logging
from pathlib import Path
from typing import (
    Any,
    List,
    Mapping,
    Optional,
    Sequence,
    TYPE_CHECKING,
    Tuple,
    Union,
)
import aiotools

import graphene
from graphql.execution.executors.asyncio import AsyncioExecutor
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession
from sqlalchemy.orm import (
    declarative_base,
    relationship,
    selectinload,
)
import trafaret as t

from ai.backend.common import redis
from ai.backend.common.docker import ImageRef
from ai.backend.common.etcd import AsyncEtcd
from ai.backend.common.exception import UnknownImageReference
from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import (
    BinarySize,
    ImageAlias,
    ResourceSlot,
)

from ai.backend.manager.container_registry import get_container_registry
import yaml

from ai.backend.manager.api.exceptions import ImageNotFound

from ai.backend.manager.defs import DEFAULT_IMAGE_ARCH

from .base import (
    BigInt, ForeignKeyIDColumn, IDColumn,
    KVPair, ResourceLimit, metadata,
)
from .user import UserRole
from .utils import ExtendedAsyncSAEngine

if TYPE_CHECKING:
    from .gql import GraphQueryContext

    from ai.backend.manager.background import ProgressReporter
    from ai.backend.manager.config import SharedConfig

log = BraceStyleAdapter(logging.getLogger(__name__))

__all__ = (
    'rescan_images',
    'update_aliases_from_file',
    'ImageRow',
    'Image',
    'PreloadImage',
    'RescanImages',
    'ForgetImage',
    'AliasImage',
    'DealiasImage',
)


# images = sa.Table(
#     'images',
#     metadata,
#     IDColumn('id'),
#     sa.Column('name', sa.String, nullable=False),
#     sa.Column('image', sa.String, nullable=False),
#     sa.Column(
#         'created_at', sa.DateTime(timezone=True),
#         server_default=sa.func.now(), index=True),
#     sa.Column('tag', sa.TEXT),
#     sa.Column('registry', sa.String, nullable=False),
#     sa.Column('architecture', sa.String, nullable=False, default='amd64'),
#     sa.Column('config_digest', sa.CHAR(length=72), nullable=False),
#     sa.Column('size_bytes', sa.Integer, nullable=False),
#     sa.Column('accelerators', sa.String, nullable=False),
#     sa.Column('labels', sa.JSON, nullable=False),
#     sa.Column('resources', sa.JSON, nullable=False),
#     sa.Column('installed', sa.Boolean, nullable=False, default=False),
# )

# image_aliases = sa.Table(
#     'image_aliases',
#     metadata,
#     sa.Column('alias', sa.String(128), primary_key=True),
#     ForeignKeyIDColumn('image', 'images.id', nullable=False),
# )


async def rescan_images(
    etcd: AsyncEtcd,
    db: ExtendedAsyncSAEngine,
    registry: str = None,
    *,
    reporter: ProgressReporter = None,
) -> None:
    # cannot import ai.backend.manager.config at start due to circular import
    from ai.backend.manager.config import container_registry_iv

    registry_config_iv = t.Mapping(t.String, container_registry_iv)
    latest_registry_config = registry_config_iv.check(
        await etcd.get_prefix('config/docker/registry'),
    )
    # TODO: delete images from registries removed from the previous config?
    if registry is None:
        # scan all configured registries
        registries = latest_registry_config
    else:
        try:
            registries = {registry: latest_registry_config[registry]}
        except KeyError:
            raise RuntimeError("It is an unknown registry.", registry)
    async with aiotools.TaskGroup() as tg:
        for registry_name, registry_info in registries.items():
            log.info('Scanning kernel images from the registry "{0}"', registry_name)
            scanner_cls = get_container_registry(registry_info)
            scanner = scanner_cls(db, registry_name, registry_info)
            tg.create_task(scanner.rescan_single_registry(reporter))
    # TODO: delete images removed from registry?


async def update_aliases_from_file(session: AsyncSession, file: Path) -> None:
    log.info('Updating image aliases from "{0}"', file)
    try:
        data = yaml.safe_load(open(file, 'r', encoding='utf-8'))
    except IOError:
        log.error('Cannot open "{0}".', file)
        return
    for item in data['aliases']:
        alias = item[0]
        target = item[1]
        if len(item) >= 2:
            architecture = item[2]
        else:
            architecture = DEFAULT_IMAGE_ARCH
        try:
            image_row = await ImageRow.from_image_ref(
                session, ImageRef(target, architecture, ['*']),
            )
            image_alias = ImageAliasRow(
                alias=alias,
                image=image_row,
            )
            # let user call session.begin()
            session.add(image_alias)
            print(f'{alias} -> {image_row.image_ref}')
        except UnknownImageReference:
            print(f'{alias} -> target image not found')
    log.info('Done.')


Base: Any = declarative_base(metadata=metadata)


class ImageRow(Base):
    __tablename__ = 'images'
    id = IDColumn('id')
    name = sa.Column('name', sa.String, nullable=False)
    image = sa.Column('image', sa.String, nullable=False)
    created_at = sa.Column(
        'created_at', sa.DateTime(timezone=True),
        server_default=sa.func.now(), index=True,
    )
    tag = sa.Column('tag', sa.TEXT)
    registry = sa.Column('registry', sa.String, nullable=False)
    architecture = sa.Column('architecture', sa.String, nullable=False, default='x86_64')
    config_digest = sa.Column('config_digest', sa.CHAR(length=72), nullable=False)
    size_bytes = sa.Column('size_bytes', sa.BigInteger, nullable=False)
    accelerators = sa.Column('accelerators', sa.String)
    labels = sa.Column('labels', sa.JSON, nullable=False)
    resources = sa.Column('resources', sa.JSON, nullable=False)
    aliases: relationship

    def __init__(
        self,
        name,
        architecture,
        registry=None,
        image=None,
        tag=None,
        config_digest=None,
        size_bytes=None,
        accelerators=None,
        labels=None,
        resources=None,
    ) -> None:
        self.name = name
        self.registry = registry
        self.image = image
        self.tag = tag
        self.architecture = architecture
        self.config_digest = config_digest
        self.size_bytes = size_bytes
        self.accelerators = accelerators
        self.labels = labels
        self.resources = resources

    @property
    def image_ref(self):
        return ImageRef(
            self.name, self.architecture,
            [self.registry],
        )

    @classmethod
    async def from_alias(
        cls,
        session: AsyncSession,
        alias: str,
        load_aliases=False,
    ) -> ImageRow:
        query = \
            sa.select(ImageRow).select_from(
                sa.join(ImageRow, ImageAliasRow, (
                    (ImageAliasRow.image == ImageRow.id) &
                    (ImageAliasRow.alias == alias)
                )),
            )
        if load_aliases:
            query = query.options(selectinload(ImageRow.aliases))
        result = await session.scalar(query)
        if result is not None:
            return result
        else:
            raise UnknownImageReference

    @classmethod
    async def from_image_ref(
        cls,
        session: AsyncSession,
        ref: ImageRef,
        strict=False,
        load_aliases=False,
    ) -> ImageRow:
        """
        loads image row with given ImageRef object.
        if `strict` is False and image table has only one row
        with respect to requested canonical, this function will
        return that row regardless of actual architecture.
        """
        log.debug('from_image_ref(): {} ({})', ref.canonical, ref.architecture)
        query = sa.select(ImageRow).where(ImageRow.name == ref.canonical)
        if load_aliases:
            query = query.options(selectinload(ImageRow.aliases))

        result = await session.execute(query)
        candidates: List[ImageRow] = result.scalars().all()
        log.debug('rows: {}', candidates)

        if len(candidates) == 0:
            raise UnknownImageReference
        if len(candidates) == 1 and not strict:
            return candidates[0]
        for row in candidates:
            log.debug('row: {}', row)
            if row.architecture == ref.architecture:
                return row
        raise UnknownImageReference

    @classmethod
    async def resolve(
        cls,
        session: AsyncConnection,
        reference_candidates: List[Union[ImageAlias, ImageRef]],
        load_aliases=False,
    ) -> ImageRow:
        """Tries to resolve matching row of image table by iterating through reference_candidates.
        If type of candidate element is `str`, it'll be considered only as an alias to image.
        Passing image canonical directly to resolve image data is no longer possible.
        You need to declare ImageRef object explicitly if you're using string
        as an image canonical. For example:
        ```
        await resolve_image_row(conn, [
            ImageRef(params['image'],
            params['architecture']), params['image']
        ])
        ```
        This kind of pattern is considered as 'good use case',
        since accepting multiple reference candidates is intended to make
        user explicitly declare that the code will first try to consider string
        as an image canonical and try to load image, and changes to alias if it fails.
        """

        for reference in reference_candidates:
            resolver_func: Any = None
            if isinstance(reference, str):
                resolver_func = cls.from_alias
            elif isinstance(reference, ImageRef):
                resolver_func = functools.partial(cls.from_image_ref, strict=False)
            if (row := await resolver_func(session, reference, load_aliases=load_aliases)):
                return row
        raise UnknownImageReference

    @classmethod
    async def list(cls, session: AsyncSession, load_aliases=False) -> List[ImageRow]:
        query = sa.select(ImageRow)
        if load_aliases:
            query = query.options(selectinload(ImageRow.aliases))
        result = await session.execute(query)
        return result.scalars().all()

    def __str__(self) -> str:
        return self.image_ref.canonical + f' ({self.image_ref.architecture})'

    async def get_slot_ranges(
        self,
        shared_config: SharedConfig,
    ) -> Tuple[ResourceSlot, ResourceSlot]:
        slot_units = await shared_config.get_resource_slots()
        min_slot = ResourceSlot()
        max_slot = ResourceSlot()

        for slot_key, resource in self.resources.items():
            slot_unit = slot_units.get(slot_key)
            if slot_unit is None:
                # ignore unknown slots
                continue
            min_value = resource.get('min')
            if min_value is None:
                min_value = Decimal(0)
            max_value = resource.get('max')
            if max_value is None:
                max_value = Decimal('Infinity')
            if slot_unit == 'bytes':
                if not isinstance(min_value, Decimal):
                    min_value = BinarySize.from_str(min_value)
                if not isinstance(max_value, Decimal):
                    max_value = BinarySize.from_str(max_value)
            else:
                if not isinstance(min_value, Decimal):
                    min_value = Decimal(min_value)
                if not isinstance(max_value, Decimal):
                    max_value = Decimal(max_value)
            min_slot[slot_key] = min_value
            max_slot[slot_key] = max_value

        # fill missing
        for slot_key in slot_units.keys():
            if slot_key not in min_slot:
                min_slot[slot_key] = Decimal(0)
            if slot_key not in max_slot:
                max_slot[slot_key] = Decimal('Infinity')

        return min_slot, max_slot

    async def _scan_reverse_aliases(self, conn: AsyncConnection) -> List[str]:
        result = await conn.execute(
            sa.select([ImageAliasRow.alias])
            .where(ImageAliasRow.image == self.id),
        )
        return result.scalars().all()

    def _parse_row(self):
        res_limits = []
        for slot_key, slot_range in self.resources.items():
            min_value = slot_range.get('min')
            if min_value is None:
                min_value = Decimal(0)
            max_value = slot_range.get('max')
            if max_value is None:
                max_value = Decimal('Infinity')
            res_limits.append({
                'key': slot_key,
                'min': min_value,
                'max': max_value,
            })

        accels = self.accelerators
        if accels is None:
            accels = []
        else:
            accels = accels.split(',')

        return {
            'canonical_ref': self.name,
            'name': self.image,
            'humanized_name': self.image,  # TODO: implement
            'tag': self.tag,
            'registry': self.registry,
            'digest': self.config_digest,
            'labels': self.labels,
            'size_bytes': self.size_bytes,
            'resource_limits': res_limits,
            'supported_accelerators': accels,
        }

    async def inspect(self, conn: AsyncConnection) -> Mapping[str, Any]:
        reverse_aliases = await self._scan_reverse_aliases(conn)
        parsed_image_info = self._parse_row()
        parsed_image_info.update('reverse_aliases', reverse_aliases)
        return parsed_image_info

    def set_resource_limit(
        self, slot_type: str,
        value_range: Tuple[Optional[Decimal], Optional[Decimal]],
    ):
        resources = self.resources
        if resources.get(slot_type) is None:
            resources[slot_type] = {}
        if value_range[0] is not None:
            resources[slot_type]['min'] = str(value_range[0])
        if value_range[1] is not None:
            resources[slot_type]['max'] = str(value_range[1])

        self.resources = resources


class ImageAliasRow(Base):
    __tablename__ = 'image_aliases'
    alias = sa.Column('alias', sa.String(128), primary_key=True)
    image_id = ForeignKeyIDColumn('image', 'images.id', nullable=False)
    image: relationship

    @classmethod
    async def create(
        cls,
        session: AsyncSession,
        alias: str,
        target: ImageRow,
    ) -> ImageAliasRow:
        existing_alias: Optional[ImageRow] = await session.scalar(
            sa.select(ImageAliasRow)
            .where(ImageAliasRow.alias == alias)
            .options(selectinload(ImageAliasRow.image)),
        )
        if existing_alias is not None:
            raise ValueError(
                f'alias already created with ({existing_alias.image})',
            )
        new_alias = ImageAliasRow(
            alias=alias,
            image_id=target.id,
        )
        session.add_all([new_alias])
        return new_alias


ImageRow.aliases = relationship('ImageAliasRow', back_populates='image')
ImageAliasRow.image = relationship('ImageRow', back_populates='aliases')


class Image(graphene.ObjectType):
    name = graphene.String()
    humanized_name = graphene.String()
    tag = graphene.String()
    registry = graphene.String()
    architecture = graphene.String()
    digest = graphene.String()
    labels = graphene.List(KVPair)
    aliases = graphene.List(graphene.String)
    size_bytes = BigInt()
    resource_limits = graphene.List(ResourceLimit)
    supported_accelerators = graphene.List(graphene.String)
    installed = graphene.Boolean()
    installed_agents = graphene.List(graphene.String)
    # legacy field
    hash = graphene.String()

    @classmethod
    async def from_row(
        cls,
        ctx: GraphQueryContext,
        row: ImageRow,
    ) -> Image:
        # TODO: add architecture
        installed = (
            await redis.execute(ctx.redis_image, lambda r: r.scard(row.name))
        ) > 0
        installed_agents = await redis.execute(
            ctx.redis_image,
            lambda r: r.smembers(row.name),
        )
        if installed_agents is None:
            installed_agents = []
        is_superadmin = (ctx.user['role'] == UserRole.SUPERADMIN)
        hide_agents = False if is_superadmin else ctx.local_config['manager']['hide-agents']
        return cls(
            name=row.image,
            humanized_name=row.image,
            tag=row.tag,
            registry=row.registry,
            architecture=row.architecture,
            digest=row.config_digest,
            labels=[
                KVPair(key=k, value=v)
                for k, v in row.labels.items()],
            aliases=row.aliases,
            size_bytes=row.size_bytes,
            resource_limits=[
                ResourceLimit(
                    key=k,
                    min=v.get('min', Decimal(0)),
                    max=v.get('max', Decimal('Infinity')),
                )
                for k, v in row.resources.items()],
            supported_accelerators=(row.accelerators or '').split(','),
            installed=installed,
            installed_agents=installed_agents if not hide_agents else None,
            # legacy
            hash=row.config_digest,
        )

    @classmethod
    async def batch_load_by_canonical(
        cls,
        graph_ctx: GraphQueryContext,
        image_names: Sequence[str],
    ) -> Sequence[Optional[Image]]:
        query = (
            sa.select(ImageRow)
            .where(ImageRow.name.in_(image_names))
            .options(selectinload(ImageRow.aliases))
        )
        async with graph_ctx.create_db_session.begin() as session:
            return [
                Image.from_row(graph_ctx, row)
                for row in (await session.scalars(query))
            ]

    @classmethod
    async def batch_load_by_image_ref(
        cls,
        graph_ctx: GraphQueryContext,
        image_refs: Sequence[ImageRef],
    ) -> Sequence[Optional[Image]]:
        image_names = [x.canonical for x in image_refs]
        return await cls.batch_load_by_canonical(graph_ctx, image_names)

    @classmethod
    async def load_item(
        cls,
        ctx: GraphQueryContext,
        reference: str,
        architecture: str,
    ) -> Image:
        try:
            async with ctx.create_db_session() as session:
                row = await ImageRow.resolve(session, [
                    ImageRef(reference, architecture, ['*']),
                    reference,
                ], load_aliases=True)
        except UnknownImageReference:
            raise ImageNotFound
        return await cls.from_row(ctx, row)

    @classmethod
    async def load_all(
        cls,
        ctx: GraphQueryContext,
        *,
        is_installed: bool = None,
        is_operation: bool = None,
    ) -> Sequence[Image]:
        async with ctx.create_db_session() as session:
            rows = await ImageRow.list(session, load_aliases=True)
        items: List[Image] = []
        # Convert to GQL objects
        for r in rows:
            item = await cls.from_row(ctx, r)
            items.append(item)
        if is_installed is not None:
            items = [*filter(lambda item: item.installed == is_installed, items)]
        if is_operation is not None:
            def _filter_operation(item):
                for label in item.labels:
                    if label.key == 'ai.backend.features' and 'operation' in label.value:
                        return not is_operation
                return not is_operation

            items = [*filter(_filter_operation, items)]
        return items

    @staticmethod
    async def filter_allowed(
        ctx: GraphQueryContext,
        items: Sequence[Image],
        domain_name: str,
        *,
        is_installed: bool = None,
        is_operation: bool = None,
    ) -> Sequence[Image]:
        from .domain import domains
        async with ctx.db.begin() as conn:
            query = (
                sa.select([domains.c.allowed_docker_registries])
                .select_from(domains)
                .where(domains.c.name == domain_name)
            )
            result = await conn.execute(query)
            allowed_docker_registries = result.scalar()
        items = [
            item for item in items
            if item.registry in allowed_docker_registries
        ]
        if is_installed is not None:
            items = [*filter(lambda item: item.installed == is_installed, items)]
        if is_operation is not None:

            def _filter_operation(item):
                for label in item.labels:
                    if label.key == 'ai.backend.features' and 'operation' in label.value:
                        return not is_operation
                return not is_operation

            items = [*filter(_filter_operation, items)]
        return items


class PreloadImage(graphene.Mutation):

    allowed_roles = (UserRole.SUPERADMIN,)

    class Arguments:
        references = graphene.List(graphene.String, required=True)
        target_agents = graphene.List(graphene.String, required=True)

    ok = graphene.Boolean()
    msg = graphene.String()
    task_id = graphene.String()

    @staticmethod
    async def mutate(
        executor: AsyncioExecutor,
        info: graphene.ResolveInfo,
        references: Sequence[str],
        target_agents: Sequence[str],
    ) -> PreloadImage:
        return PreloadImage(ok=False, msg='Not implemented.', task_id=None)


class UnloadImage(graphene.Mutation):

    allowed_roles = (UserRole.SUPERADMIN,)

    class Arguments:
        references = graphene.List(graphene.String, required=True)
        target_agents = graphene.List(graphene.String, required=True)

    ok = graphene.Boolean()
    msg = graphene.String()
    task_id = graphene.String()

    @staticmethod
    async def mutate(
        executor: AsyncioExecutor,
        info: graphene.ResolveInfo,
        references: Sequence[str],
        target_agents: Sequence[str],
    ) -> UnloadImage:
        return UnloadImage(ok=False, msg='Not implemented.', task_id=None)


class RescanImages(graphene.Mutation):

    allowed_roles = (UserRole.ADMIN, UserRole.SUPERADMIN)

    class Arguments:
        registry = graphene.String()

    ok = graphene.Boolean()
    msg = graphene.String()
    task_id = graphene.UUID()

    @staticmethod
    async def mutate(
        executor: AsyncioExecutor,
        info: graphene.ResolveInfo,
        registry: str = None,
    ) -> RescanImages:
        log.info('rescanning docker registry {0} by API request',
                 f'({registry})' if registry else '(all)')
        ctx: GraphQueryContext = info.context

        async def _rescan_task(reporter: ProgressReporter) -> None:
            await rescan_images(ctx.etcd, ctx.db, registry, reporter=reporter)

        task_id = await ctx.background_task_manager.start(_rescan_task)
        return RescanImages(ok=True, msg='', task_id=task_id)


class ForgetImage(graphene.Mutation):

    allowed_roles = (UserRole.SUPERADMIN,)

    class Arguments:
        reference = graphene.String(required=True)
        architecture = graphene.String(default_value=DEFAULT_IMAGE_ARCH)

    ok = graphene.Boolean()
    msg = graphene.String()

    @staticmethod
    async def mutate(
        executor: AsyncioExecutor,
        info: graphene.ResolveInfo,
        reference: str,
        architecture: str,
    ) -> ForgetImage:
        log.info('forget image {0} by API request', reference)
        ctx: GraphQueryContext = info.context
        async with ctx.create_db_session() as session:
            image_row = await ImageRow.resolve(session, [
                ImageRef(reference, architecture, ['*']),
                reference,
            ])
            async with session.begin():
                await session.delete(image_row)
        return ForgetImage(ok=True, msg='')


class AliasImage(graphene.Mutation):

    allowed_roles = (UserRole.SUPERADMIN,)

    class Arguments:
        alias = graphene.String(required=True)
        target = graphene.String(required=True)
        architecture = graphene.String(default_value=DEFAULT_IMAGE_ARCH)

    ok = graphene.Boolean()
    msg = graphene.String()

    @staticmethod
    async def mutate(
        executor: AsyncioExecutor,
        info: graphene.ResolveInfo,
        alias: str,
        target: str,
        architecture: str,
    ) -> AliasImage:
        image_ref = ImageRef(target, architecture, ['*'])
        log.info('alias image {0} -> {1} by API request', alias, image_ref)
        ctx: GraphQueryContext = info.context
        try:
            async with ctx.db.begin() as conn:
                image_row = await ImageRow.from_image_ref(conn, image_ref)
                if image_row is not None:
                    await image_row.alias(conn, alias)
                else:
                    raise ValueError('Image not found')
        except ValueError as e:
            return AliasImage(ok=False, msg=str(e))
        return AliasImage(ok=True, msg='')


class DealiasImage(graphene.Mutation):

    allowed_roles = (UserRole.SUPERADMIN,)

    class Arguments:
        alias = graphene.String(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @staticmethod
    async def mutate(
        executor: AsyncioExecutor,
        info: graphene.ResolveInfo,
        alias: str,
    ) -> DealiasImage:
        log.info('dealias image {0} by API request', alias)
        ctx: GraphQueryContext = info.context
        try:
            async with ctx.create_db_session() as session:
                existing_alias = await session.scalar(
                    sa.select(ImageAliasRow)
                    .where(ImageAliasRow.alias == alias),
                )
                if existing_alias is None:
                    raise DealiasImage(ok=False, msg=str('No such alias'))
                async with session.begin():
                    await session.delete(existing_alias)
        except ValueError as e:
            return DealiasImage(ok=False, msg=str(e))
        return DealiasImage(ok=True, msg='')
