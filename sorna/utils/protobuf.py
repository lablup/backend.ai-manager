#! /usr/bin/env python3

import asyncio, struct

@asyncio.coroutine
def read_message(msg_class, reader):
    msg_len = yield from reader.readexactly(2)
    msg_len = struct.unpack('>H', msg_len)
    data = yield from reader.readexactly(msg_len)
    return msg_class.ParseFromString(data)

@asyncio.coroutine
def write_message(msg, writer):
    data = output_msg.SerializeToString()
    writer.write(struct.pack('>H', len(data)))
    writer.write(data)
    yield from writer.drain()

