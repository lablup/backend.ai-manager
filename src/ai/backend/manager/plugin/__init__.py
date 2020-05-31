from typing import Mapping

from ai.backend.common.plugin import PluginRegistry


def get_plugin_handlers_by_type(plugins: Mapping[str, PluginRegistry], ev_type: str):
    """
    Yield all plugin handlers with ``ev_type`` event type.

    For example, if ``ev_type`` is ``'CHECK_USER'``, every plugin handlers
    associated with that event type will be yielded.
    """
    for plugin in plugins.values():
        hook_event_types = plugin.get_hook_event_types()
        hook_event_handlers = plugin.get_handlers()
        for ev_types in hook_event_types:
            if ev_type not in ev_types._member_names_:
                continue
            for ev_handlers in hook_event_handlers:
                for ev_handler in ev_handlers:
                    if ev_types[ev_type] == ev_handler[0]:
                        yield ev_handler[1]
