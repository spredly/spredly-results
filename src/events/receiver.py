from __future__ import annotations

import asyncio

import json
from aio_pika.abc import AbstractIncomingMessage
from aiormq import ChannelInvalidStateError
from src.events.connection import rabbitmq
from src.events.queues import Queues
from src.run_search import run
from src.logger import get_module_logger

logger = get_module_logger(__name__)


async def _process_message(message: AbstractIncomingMessage) -> None:
    async with message.process(requeue=False, ignore_processed=True):
        event = json.loads(message.body)
        event_type = event["event_type"]

        if event_type != "events_result_request":
            return

        date = event.get("date")
        if not date:
            raise ValueError("[date] is required")

        events = event.get("events", [])
        if not events:
            raise ValueError("[events] is required")

        await run(date=date, incoming_events=events)

async def receive_events() -> None:
    while True:
        try:
            await rabbitmq.connect()

            queue = await rabbitmq.declare_queue(
                Queues.MATCH_REQUESTS,
                durable=True,
                for_consumer=True,
            )

            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    try:
                        await _process_message(message)
                    except ValueError as e:
                        logger.error("[receiver] invalid message schema: %s", e)
                    except ChannelInvalidStateError as e:
                        logger.warning("[receiver] channel closed while processing: %r", e)
                    except Exception as e:
                        logger.exception("[receiver] failed to process message: %r", e)

        except Exception as e:
            logger.exception(
                "[receiver] error while receiving messages: %r",
                e,
            )
            await asyncio.sleep(5)

        finally:
            try:
                await rabbitmq.close()
            except Exception as e:
                logger.exception("[receiver] error while closing connection: %r", e)

async def main() -> None:
    await receive_events()


if __name__ == "__main__":
    asyncio.run(main())