import pytest
import simplejson as json

from sorna.gateway.exceptions import SornaError, SornaAgentError
from sorna.utils import odict


def test_sorna_error_obj():
    sorna_err = SornaError()
    assert sorna_err.args == (sorna_err.status_code, sorna_err.reason,
                              sorna_err.error_type)
    assert sorna_err.body == json.dumps(odict(
        ('type', sorna_err.error_type), ('title', sorna_err.error_title)
    )).encode()

    extra_msg = '!@#$'
    sorna_err = SornaError(extra_msg)
    assert extra_msg in sorna_err.error_title


def test_sorna_agent_error_obj():
    agt_err = SornaAgentError('timeout')

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
