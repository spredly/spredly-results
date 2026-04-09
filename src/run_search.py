from src.events.sender import send_event
from src.fuzzing import find_event
from src.parser import parse
from src.events.queues import Queues

async def run(date: str, incoming_events: list[dict]):
    candidate_events = await parse(date)
    i = 0

    compared_list = []
    for incoming_event in incoming_events:
        candidate = find_event(candidate_events=candidate_events, incoming_event=incoming_event)

        compared_list.append({
            'event_id': incoming_event.get('event_id'),
            'score1': candidate.get('score1') if candidate else None,
            'score2': candidate.get('score2') if candidate else None,
        })

        i += 1

    payload = {
        'queue': Queues.MATCH_RESULTS,
        'event_type': 'events_result_response',
        'events': compared_list,
    }
    
    await send_event(payload)

    print(f'sended {i} events')


