import asyncio
import base64
import secrets
from typing import Any

from aiohttp import web
import aiohttp_cors
import jinja2
import sqlalchemy as sa
import trafaret as t

from ai.backend.common import validators as tx

from .auth import auth_required
from .exceptions import BackendError
from ..manager import ALL_ALLOWED, server_status_required
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
    {{ runtime_path }} -m pip install ${PIP_OPTS} pillow && \
    {{ runtime_path }} -m pip install ${PIP_OPTS} h5py && \
    {{ runtime_path }} -m pip install ${PIP_OPTS} ipython && \
    {{ runtime_path }} -m pip install ${PIP_OPTS} jupyter && \
    {{ runtime_path }} -m pip install ${PIP_OPTS} jupyterlab

# Install ipython kernelspec
RUN {{ runtime_path }} \
    -m ipykernel \
    --prefix={{ runtime_path.parent.parent }} \
    install --display-name "{{ brand }} on Backend.AI" && \
    cat /usr/local/share/jupyter/kernels/python3/kernel.json
{%- endif %}
COPY {{ jail_policy_path }} /etc/backend.ai/jail/policy.yml

LABEL ai.backend.kernelspec="1" \
      ai.backend.envs.corecount="{{ cpucount_envvars | join(',') }}" \
      ai.backend.features="query batch uid-match" \
{%- if accelerators %}
      ai.backend.accelerators="{{ accelerators | join(',') }}" \
{%- endif %}
      ai.backend.resource.min.cpu="{{ min_cpu }}" \
      ai.backend.resource.min.mem="{{ min_mem }}" \
{%- if 'cuda' is in accelerators %}
      ai.backend.resource.min.cuda.device=1 \
      ai.backend.resource.min.cuda.shares=0.1 \
{%- endif %}
      ai.backend.base-distro="{{ base_distro }}" \
      ai.backend.runtime-type="{{ runtime_type }}" \
      ai.backend.runtime-path="{{ runtime_path }}" \
      ai.backend.service-ports="{{ service_ports | join(',') }}"
'''


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
        t.Key('baseDistro'): t.Enum('ubuntu', 'centos', 'alpine'),
        t.Key('minCPU'): t.Int[1:],
        t.Key('minMemory'): tx.BinarySize,
        t.Key('supportedAccelerators'): t.List(t.String),
        t.Key('runtimeType'): t.Enum('python'),
        t.Key('runtimePath'): tx.Path(allow_nonexisting=True),
        t.Key('CPUCountEnvs'): t.List(t.String),
        t.Key('servicePorts'): t.List(t.Dict({
            t.Key('name'): t.String,
            t.Key('protocol'): t.Enum('http', 'tcp', 'pty'),
            t.Key('port'): t.Int[1:65535],
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

    # TODO: validate and convert arguments to template variables
    dockerfile_content = tpl.render({
        'base_distro': 'ubuntu16.04',
        'cpucount_envvars': ['NPROC', 'OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS'],
        'runtime_type': 'python',
        'runtime_path': '/usr/local/bin/python',
        'service_ports': ['test1', 'test2'],
        'min_cpu': 1,
        'min_mem': '1g',
        'accelerators': ['cuda'],
        'jail_policy_path': '/.../policy.yml',
        'src': 'mypreciousimage:tag',
        'brand': 'My Precious Runtime',
    }).encode('utf8')

    sess_id = f'image-import-{secrets.token_urlsafe(8)}'
    access_key = request['access_key']
    registry = request.app['registry']
    resource_policy = request['keypair']['resource_policy']
    distro = 'ubuntu'

    async with request.app['dbpool'].acquire() as conn, conn.begin():
        query = (sa.select([groups.c.domain_name, groups.c.id])
                   .select_from(groups)
                   .where(domains.c.name == params['domain'])
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

    kernel, _ = await registry.get_or_create_session(
        sess_id, access_key,
        'lablup/importer:manylinux2010',
        {
            'resources': {'cpu': '1', 'mem': '1g'},
            'scaling_group': params['launchOptions']['scalingGroup'],
            'environ': {
                'SRC_IMAGE': params['src'],
                'TARGET_IMAGE': params['target'],
                'RUNTIME_PATH': params['runtimePath'],
                'BUILD_SCRIPT': (base64.b64encode(dockerfile_content)
                                 .decode('ascii')),
            }
        },
        resource_policy,
        domain_name='default',
        group_id=group_id,
        user_uuid=request['user']['uuid'],
        tag=None,
    )
    # TODO: implement agent RPC option to handle special importer kernels
    # TODO: execute "build-image.sh" in the importer kernel
    # TODO: destroy the importer kernel after it has finished
    return web.Response(text=dockerfile_content, status=200)


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
    cors.add(app.router.add_route('POST', '/import', import_image))
    return app, []
