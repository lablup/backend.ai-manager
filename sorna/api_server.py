#! /usr/bin/env python3
from sorna.proto.api_pb2 import InputMessage, OutputMessage, ActionType, ReplyType
import asyncio
import signal
import struct

@asyncio.coroutine
def handle_api(reader, writer):
    while not reader.at_eof():
        input_len     = yield from reader.readexactly(2)
        input_msg_len = struct.unpack('>H', input_len)
        input_data    = yield from reader.readexactly(input_msg_len)
        input_msg     = InputMessage.ParseFromString(input_data)
        output_msg = OutputMessage()

        if input_msg.action == ActionType.PING:
            output_msg.reply     = ReplyType.PONG
            output_msg.kernel_id = 0
            output_msg.length    = 0
        elif input_msg.action == ActionType.CREATE:
            raise NotImplementedError()
        elif input_msg.action == ActionType.DESTROY:
            raise NotImplementedError()
        elif input_msg.action == ActionType.EXECUTE:
            output_msg.reply     = ReplyType.TEXT_OUTPUT
            output_msg.kernel_id = 1
            # TODO: implement
            otuput_msg.length    = 11
            output_msg.content   = 'hello world'

        output = output_msg.SerializeToString()
        writer.write(struct.pack('>H', len(output)))
        writer.write(output)
        yield from writer.drain()

def handle_exit():
    loop.stop()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    start_coro = asyncio.start_server(handle_api, '0.0.0.0', 5001, loop=loop)
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
