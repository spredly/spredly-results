from src.fuzzing import find_event
from src.parser import parse
import functools
import asyncio
import time

async def main():
    candidate_events = await parse("2026-03-27")

    incoming_event = {
        "team1": "Жан кумстат",
        "team2": "",
        "matchId": 1626566553,
    }

    compared = find_event(candidate_events=candidate_events, incoming_event=incoming_event)
    from pprint import pprint
    pprint(compared)

asyncio.run(main())
