import base64
import secrets
from typing import Any

from aiohttp import web
import aiohttp_cors
import jinja2
import sqlalchemy as sa
import trafaret as t

from ai.backend.common import validators as tx
from ai.backend.common.docker import ImageRef
from ai.backend.common.etcd import (
    quote as etcd_quote,
)
from ai.backend.common.types import (
    SessionTypes,
)

from .auth import auth_required
from .exceptions import BackendError
from .manager import ALL_ALLOWED, READ_ALLOWED, server_status_required
from ..manager.models import domains, groups
from .utils import (
    check_api_params,
)


DOCKERFILE_TEMPLATE = r'''# syntax = docker/dockerfile:1.0-experimental
FROM {{ src }}
MAINTAINER Backend.AI Manager

{% if runtime_type == 'python' -%}
ENV PYTHONUNBUFFERED=1 \
    LANG=C.UTF-8

RUN --mount=type=bind,source=wheelhouse,target=/root/wheelhouse \
    PIP_OPTS="--no-cache-dir --no-index --find-links=/root/wheelhouse" && \
    {{ runtime_path }} -m pip install ${PIP_OPTS} -U pip setuptools && \
    {{ runtime_path }} -m pip install ${PIP_OPTS} Pillow && \
    {{ runtime_path }} -m pip install ${PIP_OPTS} h5py && \
    {{ runtime_path }} -m pip install ${PIP_OPTS} ipython && \
    {{ runtime_path }} -m pip install ${PIP_OPTS} jupyter && \
    {{ runtime_path }} -m pip install ${PIP_OPTS} jupyterlab

# Install ipython kernelspec
RUN {{ runtime_path }} -m ipykernel install \
    --prefix={{ runtime_path.parent.parent }} \
    --display-name "{{ brand }} on Backend.AI" && \
    cat /usr/local/share/jupyter/kernels/python3/kernel.json
{%- endif %}

LABEL ai.backend.kernelspec="1" \
      ai.backend.envs.corecount="{{ cpucount_envvars | join(',') }}" \
      ai.backend.features="query batch uid-match" \
      ai.backend.resource.min.cpu="{{ min_cpu }}" \
      ai.backend.resource.min.mem="{{ min_mem }}" \
      ai.backend.accelerators="{{ accelerators | join(',') }}"
{%- if 'cuda' is in accelerators %}
      ai.backend.resource.min.cuda.device=1 \
      ai.backend.resource.min.cuda.shares=0.1 \
{%- endif %}
      ai.backend.base-distro="{{ base_distro }}" \
      ai.backend.runtime-type="{{ runtime_type }}" \
      ai.backend.runtime-path="{{ runtime_path }}" \
{%- if service_ports %}
      ai.backend.service-ports="{% for item in service_ports -%}
          {{- item['name'] }}:
          {{- item['protocol'] }}:
          {%- if item['ports'] is sequence and (item['ports'] | length) > 1 -%}
              [{{- item['ports'] | join(',') -}}]
          {%- else -%}
              {{- item['ports'] -}}
          {%- endif -%}
          {{- item['port'] }}{{ ',' if not loop.last }}
      {%- endfor %}" \
{%- endif %}
'''  # noqa


@server_status_required(READ_ALLOWED)
@auth_required
async def get_import_image_form(request: web.Request) -> web.Response:
    return web.json_response({
        'fieldGroups': [
            {
                'name': 'Import options',
                'fields': [
                    {
                        'name': 'src',
                        'type': 'string',
                        'label': 'Source Docker image',
                        'placeholder': 'index.docker.io/lablup/tensorflow:2.0-source',
                        'help': 'The full Docker image name to import from. '
                                'The registry must be accessible by the client.',
                    },
                    {
                        'name': 'target',
                        'type': 'string',
                        'label': 'Target Docker image',
                        'placeholder': 'index.docker.io/lablup/tensorflow:2.0-target',
                        'help': 'The full Docker image name of the imported image.'
                                'The registry must be accessible by the client.',
                    },
                    {
                        'name': 'brand',
                        'type': 'string',
                        'label': 'Name of Jupyter kernel',
                        'placeholder': 'Tensorflow 2.0 on Backend.AI',
                        'help': 'The name of kernel to be shown in the Jupyter\'s kernel menu.',
                    },
                    {
                        'name': 'baseDistro',
                        'type': 'choice',
                        'choices': ['ubuntu', 'centos'],
                        'default': 'ubuntu',
                        'label': 'Base LINUX distribution',
                        'help': 'The base Linux distribution used by the source image',
                    },
                    {
                        'name': 'minCPU',
                        'type': 'number',
                        'min': 1,
                        'max': None,
                        'label': 'Minimum required CPU core(s)',
                        'help': 'The minimum number of CPU cores required by the image',
                    },
                    {
                        'name': 'minMemory',
                        'type': 'binarysize',
                        'min': '64m',
                        'max': None,
                        'label': 'Minimum required memory',
                        'help': 'The minimum size of the main memory required by the image',
                    },
                    {
                        'name': 'supportedAccelerators',
                        'type': 'multichoice[str]',
                        'choices': ['cuda'],
                        'default': 'cuda',
                        'label': 'Supported accelerators',
                        'help': 'The list of accelerators supported by the image',
                    },
                    {
                        'name': 'runtimeType',
                        'type': 'choice',
                        'choices': ['python'],
                        'default': 'python',
                        'label': 'Runtime type of the image',
                        'help': 'The runtime type of the image. '
                                'Currently, the source image must have installed Python 2.7, 3.5, 3.6, '
                                'or 3.7 at least to import. '
                                'This will be used as the kernel of Jupyter service in this image.',
                    },
                    {
                        'name': 'runtimePath',
                        'type': 'string',
                        'default': '/usr/local/bin/python',
                        'label': 'Path of the runtime',
                        'placeholder': '/usr/local/bin/python',
                        'help': 'The path to the main executalbe of runtime language of the image. '
                                'Even for the same "python"-based images, this may differ significantly '
                                'image by image. (e.g., /usr/bin/python, /usr/local/bin/python, '
                                '/opt/something/bin/python, ...) '
                                'Please check this carefully not to get confused with OS-default ones '
                                'and custom-installed ones.',
                    },
                    {
                        'name': 'CPUCountEnvs',
                        'type': 'list[string]',
                        'default': ['NPROC', 'OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS'],
                        'label': 'CPU count environment variables',
                        'help': 'The name of environment variables to be overriden to the number of CPU '
                                'cores actually allocated to the container. Required for legacy '
                                'computation libraries.',
                    },
                    {
                        'name': 'servicePorts',
                        'type': 'multichoice[template]',
                        'templates': [
                            {'name': 'jupyter', 'protocol': 'http', 'port': 8080},
                            {'name': 'jupyterlab', 'protocol': 'http', 'port': 8090},
                            {'name': 'tensorboard', 'protocol': 'http', 'port': [6006, 6064]},
                            {'name': 'digits', 'protocol': 'http', 'port': 5000},
                            {'name': 'vscode', 'protocol': 'http', 'port': 8180},
                            {'name': 'h2o-dai', 'protocol': 'http', 'port': 12345},
                        ],
                        'label': 'Supported service ports',
                        'help': 'The list of service ports supported by this image. '
                                'Note that sshd and ttyd are always supported regardless of '
                                'the source image.',
                    },
                ]
            },
            {
                'name': 'Import Task Options',
                'help': 'The import task uses 1 CPU core and 2 GiB of memory.',
                'fields': [
                    {
                        'name': 'domain',
                        'type': 'choice',
                        'choices': [],  # TODO: implement
                        'label': 'Domain to build image',
                        'help': 'The domain where the import task will be executed.',
                    },
                    {
                        'name': 'group',
                        'type': 'choice',
                        'choices': [],  # TODO: implement
                        'label': 'Group to build image',
                        'help': 'The user group where the import task will be executed.',
                    },
                    {
                        'name': 'scalingGroup',
                        'type': 'choice',
                        'choices': [],  # TODO: implement
                        'label': 'Scaling group to build image',
                        'help': 'The scaling group where the import task will take resources from.',
                    },
                ]
            },
        ]
    })


@server_status_required(ALL_ALLOWED)
@auth_required
@check_api_params(
    t.Dict({
        t.Key('src'): t.String,
        t.Key('target'): t.String,
        t.Key('launchOptions', default={}): t.Dict({
            t.Key('scalingGroup', default='default'): t.String,
            t.Key('group', default='default'): t.String,
        }).allow_extra('*'),
        t.Key('brand'): t.String,
        t.Key('baseDistro'): t.Enum('ubuntu', 'centos'),
        t.Key('minCPU'): t.Int[1:],
        t.Key('minMemory'): tx.BinarySize,
        t.Key('supportedAccelerators'): t.List(t.String),
        t.Key('runtimeType'): t.Enum('python'),
        t.Key('runtimePath'): tx.Path(type='file', allow_nonexisting=True),
        t.Key('CPUCountEnvs'): t.List(t.String),
        t.Key('servicePorts'): t.List(t.Dict({
            t.Key('name'): t.String,
            t.Key('protocol'): t.Enum('http', 'tcp', 'pty'),
            t.Key('port'): t.Int[1:65535] | t.List(t.Int[1:65535]),
        })),
    }).allow_extra('*'))
async def import_image(request: web.Request, params: Any) -> web.Response:
    '''
    Import a docker image and convert it to a Backend.AI-compatible one,
    by automatically installing a few packages and adding image labels.

    Currently we only support auto-conversion of Python-based kernels (e.g.,
    NGC images) which has its own Python version installed.

    Internally, it launches a temporary kernel in an arbitrary agent within
    the client's domain, the "default" group, and the "default" scaling group.
    (The client may change the group and scaling group using *launchOptions.*
    If the client is a super-admin, it uses the "default" domain.)

    This temporary kernel occupies only 1 CPU core and 1 GiB memory.
    The kernel concurrency limit is not applied here, but we choose an agent
    based on their resource availability.
    The owner of this kernel is always the client that makes the API request.

    This API returns immediately after launching the temporary kernel.
    The client may check the progress of the import task using session logs.
    '''

    tpl = jinja2.Template(DOCKERFILE_TEMPLATE)

    async with request.app['dbpool'].acquire() as conn, conn.begin():
        query = (
            sa.select([domains.c.allowed_docker_registries])
            .select_from(domains)
            .where(domains.c.name == request['user']['domain_name'])
        )
        result = await conn.execute(query)
        allowed_docker_registries = await result.scalar()

    source_image = ImageRef(params['src'], allowed_docker_registries)
    target_image = ImageRef(params['target'], allowed_docker_registries)

    # TODO: validate and convert arguments to template variables
    dockerfile_content = tpl.render({
        'base_distro': params['baseDistro'],
        'cpucount_envvars': ['NPROC', 'OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS'],
        'runtime_type': params['runtimeType'],
        'runtime_path': params['runtimePath'],
        'service_ports': params['servicePorts'],
        'min_cpu': params['minCPU'],
        'min_mem': params['minMemory'],
        'accelerators': params['supportedAccelerators'],
        'src': params['src'],
        'brand': params['brand'],
    })

    sess_id = f'image-import-{secrets.token_urlsafe(8)}'
    access_key = request['keypair']['access_key']
    registry = request.app['registry']
    resource_policy = request['keypair']['resource_policy']

    async with request.app['dbpool'].acquire() as conn, conn.begin():
        query = (
            sa.select([groups.c.domain_name, groups.c.id])
            .select_from(groups)
            .where(domains.c.name == request['user']['domain_name'])
            .where(domains.c.is_active)
            .where(groups.c.name == params['launchOptions']['group'])
            .where(groups.c.is_active))
        rows = await conn.execute(query)
        row = await rows.fetchone()
        if row is None:
            raise BackendError(
                f"{params['group']}: no such group "
                f"in domain {params['domain']}")
        params['domain'] = row.domain_name  # replace domain_name
        group_id = row.id

    importer_image = ImageRef(
        request.app['config']['manager']['importer-image'],
        allowed_docker_registries,
    )

    docker_creds = {}
    for img_ref in (source_image, target_image):
        registry_info = await request.app['config_server'].etcd.get_prefix_dict(
            f'config/docker/registry/{etcd_quote(img_ref.registry)}')
        docker_creds[img_ref.registry] = {
            'username': registry_info.get('username'),
            'password': registry_info.get('password'),
        }

    kernel_id = await registry.enqueue_session(
        sess_id, access_key,
        importer_image,
        SessionTypes.BATCH,
        {
            'resources': {'cpu': '1', 'mem': '2g'},
            'scaling_group': params['launchOptions']['scalingGroup'],
            'environ': {
                'SRC_IMAGE': source_image.canonical,
                'TARGET_IMAGE': target_image.canonical,
                'RUNTIME_PATH': params['runtimePath'],
                'BUILD_SCRIPT': (base64.b64encode(dockerfile_content.encode('utf8'))
                                 .decode('ascii')),
            }
        },
        resource_policy,
        domain_name=request['user']['domain_name'],
        group_id=group_id,
        user_uuid=request['user']['uuid'],
        user_role=request['user']['role'],
        startup_command='/root/build-image.sh',
        internal_data={
            'domain_socket_proxies': ['/var/run/docker.sock'],
            'docker_credentials': docker_creds,
            'prevent_vfolder_mounts': True,
        }
    )
    return web.json_response({
        'importTask': {
            'sessionId': sess_id,
            'taskId': str(kernel_id),
        },
    }, status=200)


async def init(app: web.Application):
    pass


async def shutdown(app: web.Application):
    pass


def create_app(default_cors_options):
    app = web.Application()
    app.on_startup.append(init)
    app.on_shutdown.append(shutdown)
    app['prefix'] = 'image'
    app['api_versions'] = (4,)
    cors = aiohttp_cors.setup(app, defaults=default_cors_options)
    cors.add(app.router.add_route('GET', '/import', get_import_image_form))
    cors.add(app.router.add_route('POST', '/import', import_image))
    return app, []
