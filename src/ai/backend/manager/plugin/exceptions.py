'''
This module defines a series of Backend.AI's plugin-specific errors.
'''
from aiohttp import web
from ai.backend.gateway.exceptions import BackendError


class PluginConfigurationError(web.HTTPBadRequest, BackendError):
    error_type  = 'https://api.backend.ai/probs/plugin-error'
    error_title = 'Bad request in plugin.'
