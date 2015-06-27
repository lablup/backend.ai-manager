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
from pprint import pprint

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

stop_reading_streams = False

@asyncio.coroutine
def handle_out_stream(sock, stream_type):
    global stop_reading_streams
    while True:
        try:
            cell_id, data = yield from sock.read()
            color = Fore.GREEN if stream_type == 'stdout' else Fore.YELLOW
            print(color + '[{0}]'.format(cell_id.decode('ascii')) + Fore.RESET, data.decode('utf8'), end='')
            sys.stdout.flush()
        except asyncio.CancelledError:
            if stop_reading_streams:
                return
            # Retry reading the socket.
            # NOTE: The subscriptions may have changed.
            continue
        except aiozmq.ZmqStreamClosed:
            break

stdout_reader_task = None
stderr_reader_task = None
last_cell_id_encoded = None

@asyncio.coroutine
def run_command(kernel_sock, stdout_sock, stderr_sock, cell_id, code_str):
    global stdout_reader_task, stderr_reader_task, last_cell_id_encoded
    cell_id_encoded = '{0}'.format(cell_id).encode('ascii')
    # In the web browser session, we may not need to explicitly subscribe/unsubscribe the cells.
    # Instead, we could just update all cells asynchronously.
    if last_cell_id_encoded is not None:
        stdout_sock.transport.unsubscribe(last_cell_id_encoded)
        stderr_sock.transport.unsubscribe(last_cell_id_encoded)
    stdout_sock.transport.subscribe(cell_id_encoded)
    stderr_sock.transport.subscribe(cell_id_encoded)
    last_cell_id_encoded = cell_id_encoded
    if stdout_reader_task is not None:
        stdout_reader_task.cancel()
    else:
        stdout_reader_task = asyncio.async(handle_out_stream(stdout_sock, 'stdout'), loop=loop)
    if stderr_reader_task is not None:
        stderr_reader_task.cancel()
    else:
        stderr_reader_task = asyncio.async(handle_out_stream(stderr_sock, 'stderr'), loop=loop)
    # Ensure that the readers proceed.
    yield from asyncio.sleep(0.01)
    req = AgentRequest()
    req.req_type = EXECUTE
    req.body     = json.dumps({
        'cell_id': cell_id,
        'code': code_str,
    })
    kernel_sock.write([req.SerializeToString()])
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
        if result['eval']:
            print(result['eval'])

@asyncio.coroutine
def run_tests(kernel_info):
    global stdout_reader_task, stderr_reader_task, stop_reading_streams
    # The scope of this method is same to the user's notebook session on the web browser.
    # kernel_sock should be mapped with AJAX calls.
    # stdout/stderr_sock should be mapped with WebSockets to asynchronously update cell output blocks.
    kernel_sock = yield from aiozmq.create_zmq_stream(zmq.REQ, connect=kernel_info['agent_sock'], loop=loop)
    stdout_sock = yield from aiozmq.create_zmq_stream(zmq.SUB, connect=kernel_info['stdout_sock'], loop=loop)
    stderr_sock = yield from aiozmq.create_zmq_stream(zmq.SUB, connect=kernel_info['stderr_sock'], loop=loop)
    stop_reading_streams = False
    c = 'a = 123\nprint(a)'
    yield from run_command(kernel_sock, stdout_sock, stderr_sock, 1, c)
    c = 'a += 1\nprint(a)'
    yield from run_command(kernel_sock, stdout_sock, stderr_sock, 2, c)
    c = 'def sum(a,b):\n  return a+b'
    yield from run_command(kernel_sock, stdout_sock, stderr_sock, 3, c)
    c = 'import sys\nprint(sum(a, 456), file=sys.stderr)'
    yield from run_command(kernel_sock, stdout_sock, stderr_sock, 4, c)
    c = 'raise RuntimeError("test")'
    yield from run_command(kernel_sock, stdout_sock, stderr_sock, 5, c)
    stop_reading_streams = True
    stdout_reader_task.cancel()
    stderr_reader_task.cancel()
    stdout_sock.close()
    stderr_sock.close()
    kernel_sock.close()

def handle_exit():
    loop.stop()

if __name__ == '__main__':
    colorama_init()
    asyncio.set_event_loop_policy(aiozmq.ZmqEventLoopPolicy())
    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGTERM, handle_exit)
    print('Requesting the API server to create a kernel...')
    kernel_id, kernel_info = loop.run_until_complete(create_kernel())
    print('The kernel {0} is created.'.format(kernel_id))
    try:
        loop.run_until_complete(run_tests(kernel_info))
    except KeyboardInterrupt:
        print()
        pass
    loop.close()
    print('Exit.')
