from __future__ import annotations

import asyncio
import queue
import threading

import aioredis

from ai.backend.gateway.defs import REDIS_STREAM_DB
from ai.backend.gateway.events import EventDispatcher
from ai.backend.manager.distributed import GlobalTimer


class TimerNode(threading.Thread):

    def __init__(self, join_delay, leave_delay, thread_idx, test_id, config, event_records) -> None:
        super().__init__()
        self.join_delay = join_delay
        self.leave_delay = leave_delay
        self.thread_idx = thread_idx
        self.test_id = test_id
        self.config = config
        self.event_records = event_records

    async def tick(self) -> None:
        self.event_records.put(f"{self.thread_idx}: tick")

    async def timer_node_async(self):
        redis = await aioredis.create_redis(
            self.config['redis']['addr'].as_sockaddr(),
            db=REDIS_STREAM_DB,
            password=(self.config['redis']['password']
                      if self.config['redis']['password'] else None),
        )
        event_dispatcher = await EventDispatcher.new(self.config)

        await asyncio.sleep(self.join_delay)
        timer = GlobalTimer(
            redis,
            event_dispatcher,
            self.test_id,
            self.tick,
            0.1,
        )
        try:
            await timer.join()
            await asyncio.sleep(self.leave_delay)
        finally:
            await timer.leave()
            redis.close()
            await redis.wait_closed()
            await event_dispatcher.close()

    def run(self):
        asyncio.run(self.timer_node_async())


def test_global_timer(test_id, test_config):
    event_records = queue.Queue()
    num_threads = 3
    num_records = 0
    threads = []
    for thread_idx in range(num_threads):
        t = TimerNode(
            thread_idx * 0.1,                         # join delay: 0.1, 0.2, 0.3
            (num_threads - thread_idx) * 0.1 + 0.51,  # leave delay: 0.3+0.5, 0.2+0.5, 0.1+0.5
            thread_idx,
            test_id,
            test_config,
            event_records,
        )
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    while True:
        try:
            tick_msg = event_records.get_nowait()
            print(tick_msg)
        except queue.Empty:
            break
        num_records += 1
    # the max diff of join-leave delay is 0.7 and interval is 0.1.
    assert num_records in (7, 8)
