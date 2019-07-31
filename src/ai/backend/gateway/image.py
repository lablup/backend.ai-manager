import asyncio
from typing import Any

from aiohttp import web
import aiohttp_cors
import jinja2
import trafaret as t

from ai.backend.common import validators as tx

from .auth import auth_required
from .manager import ALL_ALLOWED, server_status_required
from .utils import (
    check_api_params,
)


DOCKERFILE_TEMPLATE = r'''FROM {{ src }}
MAINTAINER Backend.AI Manager "autogen@backend.ai"
{% if runtime_type == 'python' -%}
ENV PYTHONUNBUFFERED=1 \
    LANG=C.UTF-8
RUN {{ runtime_path }} -m pip install --no-cache-dir -U pip setuptools && \
    {{ runtime_path }} -m pip install --no-cache-dir pillow && \
    {{ runtime_path }} -m pip install --no-cache-dir h5py && \
    {{ runtime_path }} -m pip install --no-cache-dir ipython && \
    {{ runtime_path }} -m pip install --no-cache-dir jupyter && \
    {{ runtime_path }} -m pip install --no-cache-dir jupyterlab

# Install ipython kernelspec
RUN {{ runtime_path }} \
    -m ipykernel \
    install --display-name "{{ brand }} on Backend.AI" && \
    cat /usr/local/share/jupyter/kernels/python3/kernel.json
{%- endif %}
COPY {{ jail_policy_path }} /etc/backend.ai/jail/policy.yml
LABEL ai.backend.kernelspec="1" \
      ai.backend.envs.corecount="{{ cpucount_envvars | join(',') }}" \
      ai.backend.features="query batch uid-match" \
      ai.backend.accelerators="cuda" \
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
        t.Key('brand'): t.String,
        t.Key('base-distro'): t.Enum('ubuntu', 'centos', 'alpine'),
        t.Key('min-cpu'): t.Int[1:],
        t.Key('min-mem'): tx.BinarySize,
        t.Key('supported-accelerators'): t.List(t.String),
        t.Key('runtime-type'): t.Enum('python'),
        t.Key('runtime-path'): t.String,
        t.Key('cpu-count-envvars'): t.List(t.String),
        t.Key('service-ports'): t.List(t.Dict({
            t.Key('name'): t.String,
            t.Key('protocol'): t.Enum('http', 'tcp', 'pty'),
            t.Key('port'): t.Int[1:65535],
        })),
    }))
async def import_image(request: web.Request, params: Any) -> web.Response:
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
    })
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
