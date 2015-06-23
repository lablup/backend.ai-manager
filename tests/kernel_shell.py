#! /usr/bin/env python3

# TODO: transform this as a proper test suite

from sorna.proto.api_pb2 import ManagerRequest, ManagerResponse
from sorna.proto.api_pb2 import PING, PONG, CREATE, DESTROY, SUCCESS, INVALID_INPUT, FAILURE
from sorna.proto.agent_pb2 import AgentRequest, AgentResponse
from sorna.proto.agent_pb2 import EXECUTE, SOCKET_INFO
import asyncio, zmq, aiozmq
import json
import signal
import uuid

@asyncio.coroutine
def create_kernel():
    api_sock = yield from aiozmq.create_zmq_stream(zmq.REQ, connect='tcp://127.0.0.1:5001', loop=loop)

    # Test if ping works.
    req_id = str(uuid.uuid4())
    req = ManagerRequest()
    req.action = PING
    req.kernel_id = ''
    req.body   = req_id
    api_sock.write([req.SerializeToString()])
    resp_data = yield from api_sock.read()
    resp = ManagerResponse()
    resp.ParseFromString(resp_data[0])
    assert resp.reply == PONG
    assert resp.body == req.body

    # Create a kernel instance.
    req.action = CREATE
    req.kernel_id = ''
    req.body = ''
    api_sock.write([req.SerializeToString()])
    resp_data = yield from api_sock.read()
    resp.ParseFromString(resp_data[0])
    assert resp.reply == SUCCESS
    kernel_info = json.loads(resp.body)
    return kernel_info

@asyncio.coroutine
def shell_loop(kernel_dealer):
    while True:
        try:
            line = input('>>> ')
        except EOFError:
            break
        if not line:
            break

        req = AgentRequest()
        req.req_type = EXECUTE
        req.body = line
        kernel_dealer.write([req.SerializeToString()])

        # TODO: multiplex stdout/stderr streams
        resp_data = yield from kernel_dealer.read()
        resp = AgentResponse()
        resp.ParseFromString(resp_data[0])
        print(resp.body)

def handle_exit():
    loop.stop()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    print('Contacting the API server...')
    kernel_info = loop.run_until_complete(create_kernel())
    print(kernel_info)
    print('The kernel is created. Trying to connect to it...')
    kernel_dealer = loop.run_until_complete(aiozmq.create_zmq_stream(zmq.REQ, connect='tcp://127.0.0.1:5002', loop=loop))
    try:
        loop.add_signal_handler(signal.SIGTERM, handle_exit)
        asyncio.async(shell_loop(kernel_dealer), loop=loop)
        loop.run_forever()
    except KeyboardInterrupt:
        print()
        pass
    kernel_dealer.close()
    loop.close()
    print('Exit.')
