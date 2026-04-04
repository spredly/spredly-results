from __future__ import annotations

import asyncio

import json
from aio_pika.abc import AbstractIncomingMessage
from pydantic import ValidationError
from src.events.connection import rabbitmq
from src.events.queues import Queues
from src.run_search import run

async def _process_message(message: AbstractIncomingMessage) -> None:
    try:
        event = json.loads(message.body)
        event_type = event['event_type']

        if (event_type == 'events_result'):
            date = event.get('date', None)
            if not date:
                raise ValidationError('[date] is required')

            events = event.get('events', [])
            
            if not len(events): 
                raise ValidationError('[events] is required')

            await message.ack()
            await run(date=date, incoming_events=events)


    except ValidationError as e:
        await message.reject(requeue=False)
        print(e)

    except Exception as e:
        await message.reject(requeue=False)
        print(e)

async def receive_events() -> None:
    attempts = 5

    for attempt in range(attempts):
        try:
            await rabbitmq.connect()

            queue = await rabbitmq.declare_queue(
                Queues.MATCH_REQUESTS,
                durable=True,
                for_consumer=True,
            )

            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    await _process_message(message)
        except Exception as e:
            print(e)
            await asyncio.sleep(5)
        finally:
            await rabbitmq.close()

async def main() -> None:
    await receive_events()


if __name__ == "__main__":
    asyncio.run(main())