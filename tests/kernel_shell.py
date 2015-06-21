#! /usr/bin/env python3

# TODO: transform this as a proper test suite

from sorna.proto.agent_pb2 import AgentRequest, AgentResponse, EXECUTE, SOCKET_INFO
import asyncio, zmq, aiozmq
import signal

@asyncio.coroutine
def shell_loop(dealer):
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
        dealer.write([req.SerializeToString()])

        # TODO: multiplex stdout/stderr streams
        resp_data = yield from dealer.read()
        resp = AgentResponse()
        resp.ParseFromString(resp_data[0])
        print(resp.body)

def handle_exit():
    loop.stop()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    conn_coro = aiozmq.create_zmq_stream(zmq.DEALER, connect='tcp://127.0.0.1:5002', loop=loop)
    dealer = loop.run_until_complete(conn_coro)
    # TODO: get SOCKET_INFO
    try:
        loop.add_signal_handler(signal.SIGTERM, handle_exit)
        asyncio.async(shell_loop(dealer), loop=loop)
        loop.run_forever()
    except KeyboardInterrupt:
        print()
        pass
    dealer.close()
    loop.close()
    print('Exit.')
