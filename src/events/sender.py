from __future__ import annotations

import json
from src.events.connection import rabbitmq
from src.events.queues import Queues
from src.logger import get_module_logger

logger = get_module_logger(__name__)

async def send_event(message: list[dict]) -> None:
    try:
        await rabbitmq.connect()

        await rabbitmq.declare_queue(
            Queues.MATCH_RESULTS,
            durable=True,
            for_consumer=False,
        )

        body = json.dumps(message, ensure_ascii=False).encode('utf8')
        await rabbitmq.publish_json(Queues.MATCH_RESULTS, body)
    except Exception as e:
        logger.exception("[sender] failed to send message: %s", e)
    finally:
        await rabbitmq.close()