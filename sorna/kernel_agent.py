#! /usr/bin/env python3

'''
The Sorna Kernel Agent

It manages the namespace and hooks for the Python code requested and execute it.
'''

from .proto.agent_pb2 import AgentRequest, AgentResponse, AgentReqType
from .utils.protobuf import read_message, write_message
import asyncio, zmq, aiozmq
import code
from functools import partial
import signal
import struct, types
import builtins as builtin_mod

class Kernel(object):
    '''
    The Kernel object.

    It creates a dummy module that user codes run and keeps the references to user-created objects
    (e.g., variables and functions).
    '''

    def __init__(self, ip):
        self.ip = ip

        # Initialize sockets.
        context = zmq.Context.instance()
        self.stdin_socket  = context.socket(zmq.ROUTER)
        self.stdin_port = self.stdin_socket.bind_to_random_port('tcp://{0}'.format(self.ip))
        self.stdout_socket = context.socket(zmq.PUB)
        self.stdout_port = self.stdout_socket.bind_to_random_port('tcp://{0}'.format(self.ip))
        self.stderr_socket = context.socket(zmq.PUB)
        self.stderr_port = self.stderr_socket.bind_to_random_port('tcp://{0}'.format(self.ip))

        # Initialize user module and namespaces.
        user_module = types.ModuleType('__main__',
                                       doc='Automatically created module for the interactive shell.')
        user_module.__dict__.setdefault('__builtin__', builtin_mod)
        user_module.__dict__.setdefault('__builtins__', builtin_mod)
        self.user_module = user_module
        self.user_global_ns = {}
        self.user_global_ns.setdefault('__name__', '__main__')
        self.user_ns = user_module.__dict__

    def execute_code(self, src):

        sys.stdin, orig_stdin   = self.stdin_reader, sys.stdin
        sys.stdout, orig_stdout = self.stdout_writer, sys.stdout
        sys.stderr, orig_stderr   = self.stderr_reader, sys.stderr

        try:
            # TODO: cache the compiled code in the memory
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

        sys.stdin = orig_stdin
        sys.stdout = orig_stdout
        sys.stderr = orig_stderr


@asyncio.coroutine
def handle_request(kernel, reader, writer):
    req = yield from read_message(AgentReqMessage, reader)
    resp = AgentResponse()

    if req.req_type == AgentReqType.HEARTBEAT:
        raise NotImplementedError()
    elif req.req_type == AgentReqType.SOCK_INFO:
        resp.body = json.loads({
            'stdin': 'tcp://{0}:{1}'.format(kernel.ip, kernel.stdin_port),
            'stdout': 'tcp://{0}:{1}'.format(kernel.ip, kernel.stdout_port),
            'stderr': 'tcp://{0}:{1}'.format(kernel.ip, kernel.stderr_port),
        })
    elif req.req_type == AgentReqType.EXECUTE:
        result = kernel.execute_code(req.body)
        resp.body = result

    yield from write_message(resp, writer)

def handle_exit():
    loop.stop()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    kernel = Kernel('127.0.0.1')  # for testing
    start_coro = asyncio.start_server(partial(handle_request, kernel), '0.0.0.0', 5002, loop=loop)
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
