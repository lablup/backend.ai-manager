import graphene

# from ai.backend.common.types import ImageRef

__all__ = (
    'Image',
)


class Image(graphene.ObjectType):
    name = graphene.String()
    tag = graphene.String()
    hash = graphene.String()
    # max_mem = graphene.Int()
    # max_cpu = graphene.Float()
    # max_disk = graphene.Float()
    # last_pull = GQLDateTime()

    @staticmethod
    async def load_all(etcd):
        items = []
        images = []
        kvdict = dict(await etcd.get_prefix('images'))
        for key, value in kvdict.items():
            kpath = key.split('/')
            if len(kpath) == 2 and value == '1':
                images.append(kpath[1])
        for image in images:
            tag_paths = filter(lambda k: k.startswith(f'images/{image}/tags/'),
                               kvdict.keys())
            for tag_path in tag_paths:
                tag = tag_path.split('/')[-1]
                hash_ = kvdict[tag_path]
                if hash_.startswith(':'):
                    continue
                item = Image(name=image, tag=tag, hash=hash_)
                items.append(item)
        # TODO: aliases?
        return items


class PreloadImage(graphene.Mutation):

    class Arguments:
        image_ref = graphene.String(required=True)
        target_agents = graphene.List(graphene.String, required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @staticmethod
    async def mutate(root, info, name, tag, hash):
        pass


class RegisterImage(graphene.Mutation):

    class Arguments:
        name = graphene.String(required=True)
        tag = graphene.String(required=True)
        hash = graphene.String(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @staticmethod
    async def mutate(root, info, name, tag, hash):
        pass


class DeregisterImage(graphene.Mutation):

    class Arguments:
        name = graphene.String(required=True)
        tag = graphene.String(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @staticmethod
    async def mutate(root, info, name, tag, hash):
        pass


class AliasImage(graphene.Mutation):

    class Arguments:
        alias = graphene.String(required=True)
        target = graphene.String(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @staticmethod
    async def mutate(root, info, name, tag, hash):
        pass


class RemoveImageAlias(graphene.Mutation):

    class Arguments:
        alias = graphene.String(required=True)

    ok = graphene.Boolean()
    msg = graphene.String()

    @staticmethod
    async def mutate(root, info, name, tag, hash):
        pass
