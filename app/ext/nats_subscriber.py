import asyncio
import logging
from typing import Awaitable, Callable, Optional

import msgspec
import nats
from nats.aio.client import Client as NATS
from nats.aio.subscription import Subscription
from nats.js.errors import APIError, NotFoundError
from app.schemas.event import Event

logger = logging.getLogger(__name__)

EventHandler = Callable[[Event], Awaitable[None]]


class NatsSubscriber:
    """Subscribe to JetStream subjects and dispatch decoded CloudEvents to handlers."""

    def __init__(
        self,
        *,
        servers: list[str],
        client_name: Optional[str] = None,
        client_secret: Optional[str] = None,
        stream_name: Optional[str] = None,
        durable_name: Optional[str] = None,
        prefetch: int = 256,
        manual_ack: bool = True,
    ):
        if not servers:
            raise ValueError("At least one NATS server must be provided.")

        self._servers = servers
        self._client_name = client_name
        self._client_secret = client_secret
        self._stream_name = stream_name
        self._durable_name = durable_name
        self._prefetch = prefetch
        self._manual_ack = manual_ack

        self._nc: Optional[NATS] = None
        self._js = None
        self._decoder = msgspec.json.Decoder()
        self._tasks: set[asyncio.Task] = set()
        self._subscriptions: list[Subscription] = []

    async def _ensure_conn(self) -> None:
        if self._nc and self._nc.is_connected:
            return

        self._nc = await nats.connect(
            servers=self._servers,
            user=self._client_name or None,
            password=self._client_secret or None,
            name=self._client_name or "linden-api-subscriber",
        )
        self._js = self._nc.jetstream()

    async def subscribe(
        self,
        subject: str,
        handler: EventHandler,
        *,
        stream: Optional[str] = None,
        durable_name: Optional[str] = None,
        queue: Optional[str] = None,
        auto_ack: bool = True,
    ) -> Subscription:
        """
        Subscribe to a JetStream subject and process messages with the provided handler.

        Args:
            subject: Subject pattern to subscribe to.
            handler: Async callable that receives decoded Event instances.
            stream: Optional override for JetStream stream name.
            durable_name: Optional durable consumer name.
            queue: Optional queue group.
            auto_ack: Automatically ack message after handler succeeds.
        """
        if not subject:
            raise ValueError("Subject must be provided for subscription.")

        await self._ensure_conn()
        stream_name = stream or self._stream_name
        if not stream_name:
            raise ValueError(
                "Stream name must be provided either in constructor or subscribe()."
            )

        try:
            subscription = await self._js.subscribe(
                subject=subject,
                stream=stream_name,
                durable=durable_name or self._durable_name,
                queue=queue,
                manual_ack=self._manual_ack or not auto_ack,
                pending_msgs_limit=self._prefetch,
            )
        except (APIError, NotFoundError) as exc:
            logger.error(
                "Failed to create JetStream subscription",
                exc_info=exc,
                extra={"subject": subject, "stream": stream_name},
            )
            raise

        task = asyncio.create_task(self._message_loop(subscription, handler, auto_ack))
        task.add_done_callback(self._cleanup_task)
        self._tasks.add(task)
        self._subscriptions.append(subscription)
        return subscription

    async def _message_loop(
        self,
        subscription: Subscription,
        handler: EventHandler,
        auto_ack: bool,
    ) -> None:
        try:
            while True:
                msg = await subscription.messages.get()
                if msg is None:
                    continue
                await self._process_message(msg, handler, auto_ack)
        except asyncio.CancelledError:
            logger.debug(
                "Subscription task cancelled for subject %s", subscription.subject
            )
            raise
        except Exception as exc:  # pragma: no cover - unexpected loop errors
            logger.error("Unhandled error in NATS subscription loop", exc_info=exc)

    async def _process_message(
        self,
        msg,
        handler: EventHandler,
        auto_ack: bool,
    ) -> None:
        try:
            payload = self._decoder.decode(msg.data)
            event = Event.model_validate(payload)
        except Exception as exc:
            logger.error("Failed to decode NATS message as Event", exc_info=exc)
            try:
                await msg.term()
            except Exception:  # pragma: no cover - best effort
                logger.debug("Failed to term message after decode error")
            return

        try:
            await handler(event)
            if auto_ack:
                await msg.ack()
        except Exception as exc:
            logger.error("Error while handling NATS message", exc_info=exc)
            try:
                await msg.nak()
            except Exception:  # pragma: no cover - best effort
                logger.debug("Failed to NAK message after handler error")

    def _cleanup_task(self, task: asyncio.Task) -> None:
        self._tasks.discard(task)
        try:
            task.result()
        except Exception:  # pragma: no cover - already logged inside
            pass

    async def close(self) -> None:
        """Cancel subscription tasks and close NATS connection."""
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

        for subscription in self._subscriptions:
            try:
                await subscription.unsubscribe()
            except Exception:  # pragma: no cover - best effort
                logger.debug(
                    "Failed to unsubscribe from subject %s", subscription.subject
                )
        self._subscriptions.clear()

        if self._nc:
            await self._nc.close()
            self._nc = None
            self._js = None

    async def __aenter__(self):
        await self._ensure_conn()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()
