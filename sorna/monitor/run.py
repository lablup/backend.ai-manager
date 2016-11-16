#! /usr/bin/env python3

'''
The Sorna Monitoring Daemon

It routes the API requests to kernel agents in VMs and manages the VM instance pool.
'''

import asyncio
from collections import ChainMap
import os
import random
import signal
import sys, string

import aiohttp
import simplejson as json
import zmq, aiozmq

from sorna.proto import Message
from sorna.proto.msgtypes import ManagerRequestTypes, SornaResponseTypes

API_URL = os.environ.get('SORNA_WATCHDOG_API_URL', None)
MGR_ADDR = os.environ.get('SORNA_MANAGER_ADDR', 'tcp://localhost:5001')


def randstr(len=10):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(len))


async def send_msg(loop, msg):
    with aiohttp.ClientSession(loop=loop) as sess:
        data = json.dumps(dict(ChainMap(msg, {
            'channel': '#watchdog',
            'username': 'sorna',
        })))
        async with sess.post(API_URL, data=data) as resp:
            ret = await resp.text()
            assert ret == 'ok'


def handle_signal(loop, term_ev):
    term_ev.set()
    loop.stop()


async def monitor_manager(loop, term_ev):
    last_state = 'normal'
    while not term_ev.is_set():
        try:
            await asyncio.wait_for(term_ev.wait(), 5, loop=loop)
        except asyncio.TimeoutError:
            pass
        else:
            break

        payload = randstr()
        req = Message()
        req['action'] = ManagerRequestTypes.PING
        req['body'] = payload
        cli = await aiozmq.create_zmq_stream(zmq.REQ, connect=MGR_ADDR, loop=loop)
        cli.write([req.encode()])
        try:
            resp_data = await asyncio.wait_for(cli.read(), timeout=3, loop=loop)
        except asyncio.TimeoutError:
            if last_state == 'normal':
                await send_msg(loop, {'attachments': [{
                    'pretext': 'Sorna manager has problems!',
                    'fallback': '[DANGER] Sorna manager timeout!',
                    'color': 'danger',
                    'fields': [{
                        'title': 'Ping timeout detected.',
                        'value': 'Address: {}'.format(MGR_ADDR),
                    }]
                }]})
            last_state = 'down'
        else:
            resp = Message.decode(resp_data[0])
            try:
                assert resp['reply'] == SornaResponseTypes.PONG
                assert resp['body'] == payload
            except AssertionError:
                if last_state == 'normal':
                    await send_msg(loop, {'attachments': [{
                        'pretext': 'Sorna manager has problems!',
                        'fallback': '[DANGER] Sorna manager corrupted!',
                        'color': 'danger',
                        'fields': [{
                            'title': 'Corrupted PING/PONG detected.',
                            'value': 'Address: {}'.format(MGR_ADDR),
                        }]
                    }]})
                last_state = 'down'
            else:
                if last_state != 'normal':
                    await send_msg(loop, {'attachments': [{
                        'pretext': 'Sorna manager has recovered!',
                        'fallback': '[GOOD] Sorna manager recovered!',
                        'color': 'good',
                        'fields': [{
                            'title': 'The manager is running normal.',
                            'value': 'Address: {}'.format(MGR_ADDR),
                        }]
                    }]})
                last_state = 'normal'
        cli.close()


if __name__ == '__main__':
    if not API_URL:
        print("SORNA_WATCHDOG_API_URL environment variable is not set.", file=sys.stderr)
        print("You create one using Slack's incoming webook integration.", file=sys.stderr)
        sys.exit(1)

    term_ev = asyncio.Event()
    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, handle_signal, loop, term_ev)
    loop.add_signal_handler(signal.SIGTERM, handle_signal, loop, term_ev)
    loop.create_task(monitor_manager(loop, term_ev))
    try:
        loop.run_forever()
    finally:
        loop.run_until_complete(asyncio.gather(*asyncio.Task.all_tasks(), loop=loop))
        loop.close()
        print('exit.')
