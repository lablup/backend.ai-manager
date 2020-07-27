'''
This module defines a series of Backend.AI-specific errors based on HTTP Error
classes from aiohttp.
Raising a BackendError is automatically mapped to a corresponding HTTP error
response with RFC7807-style JSON-encoded description in its response body.

In the client side, you should use "type" field in the body to distinguish
canonical error types beacuse "title" field may change due to localization and
future UX improvements.
'''

import json
from typing import (
    Optional, Union,
    Mapping,
)

from aiohttp import web


class BackendError(web.HTTPError):
    '''
    An RFC-7807 error class as a drop-in replacement of the original
    aiohttp.web.HTTPError subclasses.
    '''

    status_code: int = 500
    error_type: str  = 'https://api.backend.ai/probs/general-error'
    error_title: str = 'General Backend API Error.'

    content_type: str
    extra_msg: Optional[str]

    def __init__(self, extra_msg=None, extra_data=None, **kwargs):
        super().__init__(**kwargs)
        self.args = (self.status_code, self.reason, self.error_type)
        self.empty_body = False
        self.content_type = 'application/problem+json'
        self.extra_msg = extra_msg
        self.extra_data = extra_data
        body = {
            'type': self.error_type,
            'title': self.error_title,
        }
        if extra_msg is not None:
            body['msg'] = extra_msg
        if extra_data is not None:
            body['data'] = extra_data
        self.body = json.dumps(body).encode()

    def __str__(self):
        lines = []
        if self.extra_msg:
            lines.append(f'{self.error_title} ({self.extra_msg})')
        else:
            lines.append(self.error_title)
        if self.extra_data:
            lines.append(' -> extra_data: ' + repr(self.extra_data))
        return '\n'.join(lines)

    def __repr__(self):
        lines = []
        if self.extra_msg:
            lines.append(f'<{type(self).__name__}: {self.error_title} ({self.extra_msg})>')
        else:
            lines.append(f'<{type(self).__name__}: {self.error_title}>')
        if self.extra_data:
            lines.append(' -> extra_data: ' + repr(self.extra_data))
        return '\n'.join(lines)

    def __reduce__(self):
        return (type(self), (self.extra_msg, self.extra_data))


class GenericNotFound(web.HTTPNotFound, BackendError):
    error_type  = 'https://api.backend.ai/probs/generic-not-found'
    error_title = 'Unknown URL path.'


class GenericBadRequest(web.HTTPBadRequest, BackendError):
    error_type  = 'https://api.backend.ai/probs/generic-bad-request'
    error_title = 'Bad request.'


class RejectedByHook(web.HTTPBadRequest, BackendError):
    error_type  = 'https://api.backend.ai/probs/rejected-by-hook'
    error_title = 'Operation rejected by a hook plugin.'


class InvalidCredentials(web.HTTPBadRequest, BackendError):
    error_type  = 'https://api.backend.ai/probs/invalid-credentials'
    error_title = 'Invalid credentials for authentication.'


class GenericForbidden(web.HTTPForbidden, BackendError):
    error_type  = 'https://api.backend.ai/probs/generic-forbidden'
    error_title = 'Forbidden operation.'


class InsufficientPrivilege(web.HTTPForbidden, BackendError):
    error_type  = 'https://api.backend.ai/probs/insufficient-privilege'
    error_title = 'Insufficient privilege.'


class MethodNotAllowed(web.HTTPMethodNotAllowed, BackendError):
    error_type  = 'https://api.backend.ai/probs/method-not-allowed'
    error_title = 'HTTP Method Not Allowed.'


class InternalServerError(web.HTTPInternalServerError, BackendError):
    error_type  = 'https://api.backend.ai/probs/internal-server-error'
    error_title = 'Internal server error.'


class ServerMisconfiguredError(web.HTTPInternalServerError, BackendError):
    error_type  = 'https://api.backend.ai/probs/server-misconfigured'
    error_title = 'Service misconfigured.'


class ServiceUnavailable(web.HTTPServiceUnavailable, BackendError):
    error_type  = 'https://api.backend.ai/probs/service-unavailable'
    error_title = 'Serivce unavailable.'


class QueryNotImplemented(web.HTTPServiceUnavailable, BackendError):
    error_type  = 'https://api.backend.ai/probs/not-implemented'
    error_title = 'This API query is not implemented.'


class InvalidAuthParameters(web.HTTPBadRequest, BackendError):
    error_type  = 'https://api.backend.ai/probs/invalid-auth-params'
    error_title = 'Missing or invalid authorization parameters.'


class AuthorizationFailed(web.HTTPUnauthorized, BackendError):
    error_type  = 'https://api.backend.ai/probs/auth-failed'
    error_title = 'Credential/signature mismatch.'


class InvalidAPIParameters(web.HTTPBadRequest, BackendError):
    error_type  = 'https://api.backend.ai/probs/invalid-api-params'
    error_title = 'Missing or invalid API parameters.'


class GraphQLError(web.HTTPBadRequest, BackendError):
    error_type  = 'https://api.backend.ai/probs/graphql-error'
    error_title = 'GraphQL-generated error.'


class InstanceNotFound(web.HTTPNotFound, BackendError):
    error_type  = 'https://api.backend.ai/probs/instance-not-found'
    error_title = 'No such instance.'


class ImageNotFound(web.HTTPNotFound, BackendError):
    error_type  = 'https://api.backend.ai/probs/image-not-found'
    error_title = 'No such environment image.'


class GroupNotFound(web.HTTPNotFound, BackendError):
    error_type  = 'https://api.backend.ai/probs/group-not-found'
    error_title = 'No such user group.'


class ScalingGroupNotFound(web.HTTPNotFound, BackendError):
    error_type  = 'https://api.backend.ai/probs/scaling-group-not-found'
    error_title = 'No such scaling group.'


class SessionNotFound(web.HTTPNotFound, BackendError):
    error_type  = 'https://api.backend.ai/probs/session-not-found'
    error_title = 'No such session.'


class TooManySessionsMatched(web.HTTPNotFound, BackendError):
    error_type  = 'https://api.backend.ai/probs/too-many-sessions-matched'
    error_title = 'Too many sessions matched.'


class TaskTemplateNotFound(web.HTTPNotFound, BackendError):
    error_type  = 'https://api.backend.ai/probs/task-template-not-found'
    error_title = 'No such task template.'


class TooManyKernelsFound(web.HTTPNotFound, BackendError):
    error_type  = 'https://api.backend.ai/probs/too-many-kernels'
    error_title = 'There are two or more matching kernels.'


class AppNotFound(web.HTTPNotFound, BackendError):
    error_type  = 'https://api.backend.ai/probs/app-not-found'
    error_title = 'No such app service provided by the session.'


class SessionAlreadyExists(web.HTTPBadRequest, BackendError):
    error_type  = 'https://api.backend.ai/probs/session-already-exists'
    error_title = 'The session already exists but you requested not to reuse existing one.'


class VFolderCreationFailed(web.HTTPBadRequest, BackendError):
    error_type  = 'https://api.backend.ai/probs/vfolder-creation-failed'
    error_title = 'Virtual folder creation has failed.'


class VFolderNotFound(web.HTTPNotFound, BackendError):
    error_type  = 'https://api.backend.ai/probs/vfolder-not-found'
    error_title = 'No such virtual folder.'


class VFolderAlreadyExists(web.HTTPBadRequest, BackendError):
    error_type  = 'https://api.backend.ai/probs/vfolder-already-exists'
    error_title = 'The virtual folder already exists with the same name.'


class DotfileCreationFailed(web.HTTPBadRequest, BackendError):
    error_type  = 'https://api.backend.ai/probs/generic-bad-request'
    error_title = 'Dotfile creation has failed.'


class DotfileAlreadyExists(web.HTTPBadRequest, BackendError):
    error_type  = 'https://api.backend.ai/probs/generic-bad-request'
    error_title = 'Dotfile already exists.'


class DotfileNotFound(web.HTTPNotFound, BackendError):
    error_type  = 'https://api.backend.ai/probs/generic-not-found'
    error_title = 'Requested Dotfile not found.'


class QuotaExceeded(web.HTTPPreconditionFailed, BackendError):
    error_type  = 'https://api.backend.ai/probs/quota-exceeded'
    error_title = 'You have reached your resource limit.'


class RateLimitExceeded(web.HTTPTooManyRequests, BackendError):
    error_type  = 'https://api.backend.ai/probs/rate-limit-exceeded'
    error_title = 'You have reached your API query rate limit.'


class InstanceNotAvailable(web.HTTPServiceUnavailable, BackendError):
    error_type  = 'https://api.backend.ai/probs/instance-not-available'
    error_title = 'There is no available instance.'


class ServerFrozen(web.HTTPServiceUnavailable, BackendError):
    error_type  = 'https://api.backend.ai/probs/server-frozen'
    error_title = 'The server is frozen due to maintenance. Please try again later.'


class AgentError(RuntimeError):
    '''
    A dummy exception class to distinguish agent-side errors passed via
    agent rpc calls.

    It carries two args tuple: the exception type and exception arguments from
    the agent.
    '''

    def __init__(self, *args, exc_repr: str = None):
        super().__init__(*args)
        self.exc_repr = exc_repr


class BackendAgentError(BackendError):
    '''
    An RFC-7807 error class that wraps agent-side errors.
    '''

    _short_type_map = {
        'TIMEOUT': 'https://api.backend.ai/probs/agent-timeout',
        'INVALID_INPUT': 'https://api.backend.ai/probs/agent-invalid-input',
        'FAILURE': 'https://api.backend.ai/probs/agent-failure',
    }

    def __init__(self, agent_error_type: str,
                 exc_info: Union[str, AgentError, Exception, Mapping[str, Optional[str]], None] = None):
        super().__init__()
        agent_details: Mapping[str, Optional[str]]
        if not agent_error_type.startswith('https://'):
            agent_error_type = self._short_type_map[agent_error_type.upper()]
        self.args = (
            self.status_code,
            self.reason,
            self.error_type,
            agent_error_type,
        )
        if isinstance(exc_info, str):
            agent_details = {
                'type': agent_error_type,
                'title': exc_info,
            }
        elif isinstance(exc_info, AgentError):
            if exc_info.exc_repr:
                exc_repr = exc_info.exc_repr
            else:
                if isinstance(exc_info.args[0], Exception):
                    inner_name = type(exc_info.args[0]).__name__
                elif (isinstance(exc_info.args[0], type) and
                      issubclass(exc_info.args[0], Exception)):
                    inner_name = exc_info.args[0].__name__
                else:
                    inner_name = str(exc_info.args[0])
                inner_args = ', '.join(repr(a) for a in exc_info.args[1])
                exc_repr = f'{inner_name}({inner_args})'
            agent_details = {
                'type': agent_error_type,
                'title': 'Agent-side exception occurred.',
                'exception': exc_repr,
            }
        elif isinstance(exc_info, Exception):
            agent_details = {
                'type': agent_error_type,
                'title': 'Unexpected exception ocurred.',
                'exception': repr(exc_info),
            }
        elif isinstance(exc_info, Mapping):
            agent_details = exc_info
        else:
            agent_details = {
                'type': agent_error_type,
                'title': None if exc_info is None else str(exc_info),
            }
        self.agent_details = agent_details
        self.agent_error_type = agent_error_type
        self.agent_error_title = agent_details['title']
        self.agent_exception = agent_details.get('exception', '')
        self.body = json.dumps({
            'type': self.error_type,
            'title': self.error_title,
            'agent-details': agent_details,
        }).encode()

    def __str__(self):
        if self.agent_exception:
            return f'{self.agent_error_title} ({self.agent_exception})'
        return f'{self.agent_error_title}'

    def __repr__(self):
        if self.agent_exception:
            return f'<{type(self).__name__}: {self.agent_error_title} ({self.agent_exception})>'
        return f'<{type(self).__name__}: {self.agent_error_title}>'

    def __reduce__(self):
        return (type(self), (self.agent_error_type, self.agent_details))


class KernelCreationFailed(web.HTTPInternalServerError, BackendAgentError):
    error_type  = 'https://api.backend.ai/probs/kernel-creation-failed'
    error_title = 'Kernel creation has failed.'


class KernelDestructionFailed(web.HTTPInternalServerError, BackendAgentError):
    error_type  = 'https://api.backend.ai/probs/kernel-destruction-failed'
    error_title = 'Kernel destruction has failed.'


class KernelRestartFailed(web.HTTPInternalServerError, BackendAgentError):
    error_type  = 'https://api.backend.ai/probs/kernel-restart-failed'
    error_title = 'Kernel restart has failed.'


class KernelExecutionFailed(web.HTTPInternalServerError, BackendAgentError):
    error_type  = 'https://api.backend.ai/probs/kernel-execution-failed'
    error_title = 'Executing user code in the kernel has failed.'
