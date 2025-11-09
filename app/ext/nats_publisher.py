import asyncio
import logging
from typing import Optional

import msgspec
import nats
from nats.aio.client import Client as NATS
from nats.js.errors import APIError, NoStreamResponseError
from nats.errors import (
    ConnectionClosedError,
    NoServersError,
    TimeoutError as NatsTimeoutError,
)

logger = logging.getLogger(__name__)


class NatsPublisher:
    def __init__(
        self, *, servers, client_name=None, client_secret=None, publish_timeout=2.0
    ):
        self._servers = servers
        self._client_name = client_name
        self._client_secret = client_secret
        self._publish_timeout = publish_timeout
        self._nc: Optional[NATS] = None
        self._js = None
        self._encode = msgspec.json.Encoder().encode  # preallocate

    async def _ensure_conn(self) -> None:
        if self._nc and self._nc.is_connected:
            return
        self._nc = await nats.connect(
            servers=self._servers,
            user=self._client_name or None,
            password=self._client_secret or None,
            name=self._client_name or "linden-api",
        )
        self._js = self._nc.jetstream()

    async def _publish_async(self, subject: str, event) -> None:
        if not subject:
            raise ValueError("Subject must be provided for NATS publishing.")
        payload = self._encode(event.model_dump(mode="json"))
        try:
            await self._ensure_conn()
            ack = await self._js.publish(
                subject=subject,
                payload=payload,
                timeout=self._publish_timeout,
            )
            if not ack:
                raise RuntimeError("No PubAck returned from JetStream")
            logger.debug(
                "Published NATS event",
                extra={
                    "subject": subject,
                    "stream": ack.stream,
                    "sequence": ack.seq,
                    "event_type": event.type,
                },
            )
        except asyncio.CancelledError:
            raise
        except (
            NoServersError,
            ConnectionClosedError,
            NatsTimeoutError,
            APIError,
            NoStreamResponseError,
        ) as exc:
            logger.error(
                "Failed to publish NATS event",
                exc_info=exc,
                extra={"subject": subject, "event_type": getattr(event, "type", None)},
            )
            raise

    def publish_event(self, subject: str, event) -> None:
        coro = self._publish_async(subject, event)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(coro)
            return
        task = loop.create_task(coro)
        task.add_done_callback(self._log_task_exception)

    async def close(self) -> None:
        if self._nc:
            await self._nc.close()  # use drain() only for graceful shutdown of active subs
            self._nc = None
            self._js = None

    @staticmethod
    def _log_task_exception(task: asyncio.Task) -> None:
        try:
            task.result()
        except Exception as exc:
            logger.error("Unhandled exception in NATS publish task", exc_info=exc)
