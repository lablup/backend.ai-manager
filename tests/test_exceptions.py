import pytest
import simplejson as json

from backend.ai.gateway.exceptions import Backend.AiError, Backend.AiAgentError
from backend.ai.utils import odict


def test_backend.ai_error_obj():
    backend.ai_err = Backend.AiError()
    assert backend.ai_err.args == (backend.ai_err.status_code, backend.ai_err.reason,
                              backend.ai_err.error_type)
    assert backend.ai_err.body == json.dumps(odict(
        ('type', backend.ai_err.error_type), ('title', backend.ai_err.error_title)
    )).encode()

    extra_msg = '!@#$'
    backend.ai_err = Backend.AiError(extra_msg)
    assert extra_msg in backend.ai_err.error_title


def test_backend.ai_agent_error_obj():
    agt_err = Backend.AiAgentError('timeout')

    assert agt_err.args == (agt_err.status_code, agt_err.reason,
                            agt_err.error_type, agt_err.agent_error_type)
    assert agt_err.body == json.dumps(odict(
        ('type', agt_err.error_type),
        ('title', agt_err.error_title),
        ('agent-details', odict(
            ('type', agt_err.agent_error_type),
            ('title', agt_err.agent_error_title),
        )),
    )).encode()
