import simplejson as json

from ai.backend.gateway.exceptions import BackendError, BackendAgentError
from ai.backend.common.utils import odict


def test_backend_error_obj():
    eobj = BackendError()
    assert eobj.args == (eobj.status_code, eobj.reason, eobj.error_type)
    assert eobj.body == json.dumps(odict(
        ('type', eobj.error_type), ('title', eobj.error_title)
    )).encode()

    extra_msg = '!@#$'
    eobj = BackendError(extra_msg)
    assert extra_msg in eobj.error_title


def test_backend_agent_error_obj():
    eobj = BackendAgentError('timeout')

    assert eobj.args == (eobj.status_code, eobj.reason,
                         eobj.error_type, eobj.agent_error_type)
    assert eobj.body == json.dumps(odict(
        ('type', eobj.error_type),
        ('title', eobj.error_title),
        ('agent-details', odict(
            ('type', eobj.agent_error_type),
            ('title', eobj.agent_error_title),
        )),
    )).encode()
