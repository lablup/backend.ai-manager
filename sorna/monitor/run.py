#! /usr/bin/env python3

'''
The Sorna Monitoring Daemon

It routes the API requests to kernel agents in VMs and manages the VM instance pool.
'''

import argparse
import asyncio
import aiohttp
import json
import os
import sys

API_URL = os.environ.get('SORNA_WATCHDOG_API_URL', None)

async def send_msg(loop, type, text):
    with aiohttp.ClientSession(loop=loop) as sess:
        data = json.dumps({
            'channel': '#watchdog',
            'username': 'sorna',
            'text': text,
        })
        with aiohttp.Timeout(3):
            async with sess.post(API_URL, data=data) as resp:
                ret = await resp.text()
                assert ret == 'ok'

async def monitor_manager(loop):
    await send_msg(loop, 'test', 'hello world')


if __name__ == '__main__':
    if not API_URL:
        print("SORNA_WATCHDOG_API_URL environment variable is not set.", file=sys.stderr)
        print("You create one using Slack's incoming webook integration.", file=sys.stderr)
        sys.exit(1)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(monitor_manager(loop))
