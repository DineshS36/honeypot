from __future__ import annotations

import asyncio
from collections import deque
from typing import AsyncGenerator

from shared.event_schema import TelemetryEvent


class EventBroadcaster:

    def __init__(self, max_events: int = 1000) -> None:
        self._events: deque[TelemetryEvent] = deque(maxlen=max_events)
        self._condition = asyncio.Condition()

    async def publish(self, event: TelemetryEvent) -> None:
        async with self._condition:
            self._events.append(event)
            self._condition.notify_all()

    async def stream(self) -> AsyncGenerator[TelemetryEvent, None]:
        index = 0

        while True:
            async with self._condition:

                while index >= len(self._events):
                    await self._condition.wait()

                event = list(self._events)[index]
                index += 1

            yield event