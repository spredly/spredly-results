from src.events.sender import send_event
from src.fuzzing import find_event
from src.parser import parse

async def run(date: str, incoming_events: list[dict]):
    candidate_events = await parse(date)
    i = 0
    for incoming_event in incoming_events:
        compared = find_event(candidate_events=candidate_events, incoming_event=incoming_event)
        await send_event(compared)
        i += 1
    print(f'sended {i} events')

