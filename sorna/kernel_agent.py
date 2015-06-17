#! /usr/bin/env python3

'''
The Sorna Kernel Agent

It manages the namespace and hooks for the Python code requested and execute it.
'''

from sorna.proto.api_pb2 import InputMessage, OutputMessage, ActionType, ReplyType
import asyncio, zmq, aiozmq
import code
import signal
import struct, types
import builtins as builtin_mod

class Kernel(object):
    '''
    The Kernel object.

    It creates a dummy module that user codes run and keeps the references to user-created objects
    (e.g., variables and functions).
    '''

    def __init__(self):
        context = zmq.Context.instance()
        self.stdin_socket  = context.socket(zmq.ROUTER)
        self.stdin_socket.bind()
        self.stdout_socket = context.socket(zmq.PUB)
        self.stdout_socket_bind()

        user_module = types.ModuleType('__main__',
                                       doc='Automatically created module for the interactive shell.')
        user_module.__dict__.setdefault('__builtin__', builtin_mod)
        user_module.__dict__.setdefault('__builtins__', builtin_mod)
        self.user_module = user_module
        self.user_global_ns = {}
        self.user_globa_ns.setdefault('__name__', '__main__')
        self.user_ns = user_module.__dict__

    def execute_code(self, src):
        try:
            code_obj = code.compile(src)
        except IndentationError as e:
            raise
        except (OverflowError, SyntaxError, ValueError, TypeError, MemoryError) as e:
            raise

        try:
            exec(code_obj, user_global_ns, user_ns)
        except SystemExit as e:
            raise RuntimeError('You cannot shut-down the Python environment.')
        except:
            raise

        # TODO: wrap exceptions as a structured reply


@asyncio.coroutine
def handle_request(reader, writer):
    raise NotImplementedError()  # TODO: implement

def handle_exit():
    loop.stop()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    start_coro = asyncio.start_server(handle_request, '0.0.0.0', 5002, loop=loop)
    server = loop.run_until_complete(start_coro)
    print('Started serving...')
    try:
        loop.add_signal_handler(signal.SIGTERM, handle_exit)
        loop.run_forever()
    except KeyboardInterrupt:
        print()
        pass
    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()
    print('Exit.')
