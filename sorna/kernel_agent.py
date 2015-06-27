#! /usr/bin/env python3

'''
The Sorna Kernel Agent

It manages the namespace and hooks for the Python code requested and execute it.
'''

from .proto.agent_pb2 import AgentRequest, AgentResponse, HEARTBEAT, SOCKET_INFO, EXECUTE
from .utils.protobuf import read_message, write_message
import asyncio, zmq, aiozmq
import argparse
import builtins as builtin_mod
import code
from functools import partial
import json
from namedlist import namedlist, namedtuple
import signal
import struct, types
import sys
import uuid

ExceptionInfo = namedtuple('ExceptionInfo', [
    'exc',
    ('raised_before_exec', False),
    ('traceback', None),
])

class SockWriter(object):
    def __init__(self, sock):
        self._sock = sock

    def write(self, s):
        # TODO: wrap each string as a structured message
        #       (to distinguish which cell sent the output in the frontend)
        self._sock.send(s)

class SockReader(object):
    def __init__(self, sock):
        self._sock = sock

    def read(self, n):
        # TODO: derwap string from a structured message
        #       (the frontend should tag which cell sent this input)
        return self._sock.read(n)

class Kernel(object):
    '''
    The Kernel object.

    It creates a dummy module that user codes run and keeps the references to user-created objects
    (e.g., variables and functions).
    '''

    def __init__(self, ip, kernel_id):
        self.ip = ip
        self.kernel_id = kernel_id

        # Initialize sockets.
        context = zmq.Context.instance()
        self.stdin_socket  = context.socket(zmq.ROUTER)
        self.stdin_port = self.stdin_socket.bind_to_random_port('tcp://{0}'.format(self.ip))
        self.stdout_socket = context.socket(zmq.PUB)
        self.stdout_port = self.stdout_socket.bind_to_random_port('tcp://{0}'.format(self.ip))
        self.stderr_socket = context.socket(zmq.PUB)
        self.stderr_port = self.stderr_socket.bind_to_random_port('tcp://{0}'.format(self.ip))

        self.stdin_reader = SockReader(self.stdin_socket)
        self.stdout_writer = SockWriter(self.stdout_socket)
        self.stderr_writer = SockWriter(self.stdout_socket)

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

        # TODO: limit the scope of changed sys.std*
        #       (use a proxy object for sys module?)
        #sys.stdin, orig_stdin   = self.stdin_reader, sys.stdin
        #sys.stdout, orig_stdout = self.stdout_writer, sys.stdout
        #sys.stderr, orig_stderr = self.stderr_writer, sys.stderr

        exec_result = None
        exceptions = []
        before_exec = True

        def my_excepthook(type_, value, tb):
            exception.append(ExceptionInfo(value, before_exec, tb))
        sys.excepthook = my_excepthook

        try:
            # TODO: cache the compiled code in the memory
            # TODO: attach traceback in a structured format
            code_obj = code.compile_command(src, symbol='eval')
        except IndentationError as e:
            exceptions.append(ExceptionInfo(e, before_exec, None))
        except (OverflowError, SyntaxError, ValueError, TypeError, MemoryError) as e:
            exceptions.append(ExceptionInfo(e, before_exec, None))
        else:
            before_exec = False
            try:
                # TODO: distinguish whethe we should do exec or eval...
                exec_result = exec(code_obj, self.user_global_ns, self.user_ns)
            except Exception as e:
                exceptions.append(ExceptionInfo(e, before_exec, None))

        sys.excepthook = sys.__excepthook__

        #sys.stdin = orig_stdin
        #sys.stdout = orig_stdout
        #sys.stderr = orig_stderr

        return exec_result, exceptions


@asyncio.coroutine
def handle_request(loop, server, kernel):
    while True:
        req_data = yield from server.read()
        req = AgentRequest()
        req.ParseFromString(req_data[0])
        resp = AgentResponse()

        if req.req_type == HEARTBEAT:
            print('[{0}] HEARTBEAT'.format(kernel.kernel_id))
            resp.body = req.body
        elif req.req_type == SOCKET_INFO:
            print('[{0}] SOCKET_INFO'.format(kernel.kernel_id))
            resp.body = json.dumps({
                'stdin': 'tcp://{0}:{1}'.format(kernel.ip, kernel.stdin_port),
                'stdout': 'tcp://{0}:{1}'.format(kernel.ip, kernel.stdout_port),
                'stderr': 'tcp://{0}:{1}'.format(kernel.ip, kernel.stderr_port),
            })
        elif req.req_type == EXECUTE:
            print('[{0}] EXECUTE'.format(kernel.kernel_id))
            result, exceptions = kernel.execute_code(req.body)
            resp.body = json.dumps({
                'eval': str(result),
                'exceptions': ['{0!r}'.format(e) for e in exceptions],
            })
        else:
            assert False, 'Invalid kernel request type.'

        server.write([resp.SerializeToString()])

def handle_exit():
    loop.stop()


if __name__ == '__main__':
    argparser = argparse.ArgumentParser()
    argparser.add_argument('--kernel-id', default=None)
    argparser.add_argument('--agent-port', type=int, default=5002)
    args = argparser.parse_args()

    kernel_id = args.kernel_id if args.kernel_id else str(uuid.uuid4())
    kernel = Kernel('127.0.0.1', kernel_id)  # for testing
    agent_addr = 'tcp://*:{0}'.format(args.agent_port)

    asyncio.set_event_loop_policy(aiozmq.ZmqEventLoopPolicy())
    loop = asyncio.get_event_loop()
    server = loop.run_until_complete(aiozmq.create_zmq_stream(zmq.REP, bind=agent_addr, loop=loop))
    print('[{0}] Started serving at {1}'.format(kernel_id, agent_addr))
    loop.add_signal_handler(signal.SIGTERM, handle_exit)
    try:
        asyncio.async(handle_request(loop, server, kernel), loop=loop)
        loop.run_forever()
    except KeyboardInterrupt:
        print()
        pass
    server.close()
    loop.close()
    print('Exit.')
