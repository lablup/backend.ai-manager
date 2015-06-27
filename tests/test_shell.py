#! /usr/bin/env python3

# TODO: transform this as a proper test suite

from sorna.proto.api_pb2 import ManagerRequest, ManagerResponse
from sorna.proto.api_pb2 import PING, PONG, CREATE, DESTROY, SUCCESS, INVALID_INPUT, FAILURE
from sorna.proto.agent_pb2 import AgentRequest, AgentResponse
from sorna.proto.agent_pb2 import EXECUTE, SOCKET_INFO
import asyncio, zmq, aiozmq
from colorama import init as colorama_init, Fore
import json
import signal
import sys
import uuid

@asyncio.coroutine
def create_kernel():
    api_sock = yield from aiozmq.create_zmq_stream(zmq.REQ, connect='tcp://127.0.0.1:5001', loop=loop)

    # Test if ping works.
    req_id = str(uuid.uuid4())
    req = ManagerRequest()
    req.action    = PING
    req.kernel_id = ''
    req.body      = req_id
    api_sock.write([req.SerializeToString()])
    resp_data = yield from api_sock.read()
    resp = ManagerResponse()
    resp.ParseFromString(resp_data[0])
    assert resp.reply == PONG
    assert resp.body == req.body

    # Create a kernel instance.
    req.action    = CREATE
    req.kernel_id = ''
    req.body      = ''
    api_sock.write([req.SerializeToString()])
    resp_data = yield from api_sock.read()
    resp.ParseFromString(resp_data[0])
    assert resp.reply == SUCCESS
    api_sock.close()
    return resp.kernel_id, json.loads(resp.body)

@asyncio.coroutine
def run_command(kernel_sock, command_str):
    req = AgentRequest()
    req.req_type = EXECUTE
    req.body     = command_str
    kernel_sock.write([req.SerializeToString()])
    # TODO: multiplex stdout/stderr streams
    resp_data = yield from kernel_sock.read()
    resp = AgentResponse()
    resp.ParseFromString(resp_data[0])
    result = json.loads(resp.body)
    if len(result['exceptions']) > 0:
        out = []
        for e in result['exceptions']:
            out.append(str(e))
        print(Fore.RED + '\n'.join(out) + Fore.RESET, file=sys.stderr)
    else:
        print(result['eval'])

@asyncio.coroutine
def shell_loop(kernel_sock):
    c = 'a = 123\nprint(a)'
    yield from run_command(kernel_sock, c)
    c = 'a += 1\nprint(a)'
    yield from run_command(kernel_sock, c)
    c = 'def sum(a,b):\n  return a+b\n'
    yield from run_command(kernel_sock, c)
    c = 'sum(a, 456)'
    yield from run_command(kernel_sock, c)

def handle_exit():
    loop.stop()

if __name__ == '__main__':
    colorama_init()
    asyncio.set_event_loop_policy(aiozmq.ZmqEventLoopPolicy())
    loop = asyncio.get_event_loop()
    print('Contacting the API server...')
    kernel_id, kernel_info = loop.run_until_complete(create_kernel())
    print(kernel_id, kernel_info)
    print('The kernel is created. Trying to connect to it...')
    kernel_sock = loop.run_until_complete(aiozmq.create_zmq_stream(zmq.REQ, connect=kernel_info['agent_sock'], loop=loop))
    loop.add_signal_handler(signal.SIGTERM, handle_exit)
    try:
        loop.run_until_complete(shell_loop(kernel_sock))
    except KeyboardInterrupt:
        print()
        pass
    kernel_sock.close()
    loop.close()
    print('Exit.')
