from ai.backend.gateway.exceptions import AgentError


def prettify_agent_error(e: Exception, is_debug: bool = False) -> str:
    if isinstance(e, AgentError):
        # e.args is composed of (agent_id, name, repr, traceback)
        agent_id = e.args[0]
        orig_exc_repr = e.args[2]
        if is_debug:
            return f"AgentError({agent_id}): {orig_exc_repr}"
        return f"AgentError: {orig_exc_repr}"
    return repr(e)
