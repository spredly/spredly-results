from aiohttp import ClientSession, ClientConnectionError, ClientResponseError
from src.utils import extract_by_schema, resolve_competition
import asyncio

event_status_map = {
    2: 'finished',
    3: 'interrupted',
    4: 'cancelled',
}

sport_schema = {
    "id": True,
    "name": True,
}

competition_schema = {
    "id": True,
    "name": True,
    "sportId": True
}

event_schema = {
    "id": True,
    "parentId": True,
    "competitionId": True,
    "name": True,
    "startTime": True,
    "status": True,
    "team1": True,
    "team2": True,
}

event_misc_schema = {
    "firstGoal": {
        "team": True,
        "minute": True
    }, 
    "id": True,
    "score1": True,
    "score2": True,
    "winningTeam": True,
    "subScores": [
        {
            "score1": True,
            "score2": True,
            "scoreIndex": True,
            "title": True,
        }
    ]
}

async def _load_date(date: str):
    headers = {
        "accept": "*/*",
        "accept-language": "ru,ru-RU;q=0.9,en-US;q=0.8,en;q=0.7",
        "cache-control": "no-cache",
        "origin": "https://fon.bet",
        "pragma": "no-cache",
        "priority": "u=1, i",
        "referer": "https://fon.bet/",
        "sec-ch-ua": '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Linux"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "cross-site",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    }

    params = {
        "lang": "en",
        "packetVersion": "0",
        "scopeMarket": "1600",
    }

    params["lineDate"] = date

    url = "https://clientsapi-lb51-w.bk6bba-resources.com/results/v2/getByDate"

    async with ClientSession() as session:
        try:
            async with session.get(url, params=params, headers=headers) as response:
                status = response.status
                if status == 200:
                    response_json = await response.json()
                    return response_json
                else:
                    print(status)
        except ClientConnectionError as e:
            print(str(e))
        except ClientResponseError as e:
            print(str(e))

def _build(sports, competitions, events, event_miscs):
    id_to_sport = extract_by_schema(data=sports, schema=sport_schema, map_key='id')
    id_to_competition = extract_by_schema(data=competitions, schema=competition_schema, map_key='id')
    id_to_event = extract_by_schema(data=events, schema=event_schema, map_key="id")
    id_to_misc = extract_by_schema(data=event_miscs, schema=event_misc_schema, map_key="id")

    merged = []

    for event_id, event in id_to_event.items():
        competition = resolve_competition(event, id_to_event, id_to_competition)
        sport = id_to_sport.get(competition["sportId"]) if competition else None
        misc = id_to_misc.get(event_id, {})
        first_goal = misc.get("firstGoal") or {}

        merged.append({
            "event_id": event.get("id"),
            "parent_id": event.get("parent_id"),

            "sport_name": sport.get("name") if sport else None,

            "competition_id": competition.get("id") if competition else None,
            "competition_name": competition.get("name") if competition else None,

            "event_name": event.get("name"),
            "start_time": event.get("startTime"),

            "status_id": event.get("status"),
            "status_name": event_status_map.get(event.get("status"), "unknown"),

            "home_team_name": event.get("team1"),
            "away_team_name": event.get("team2"),

            "score1": misc.get("score1"),
            "score2": misc.get("score2"),
            "winning_team": misc.get("winningTeam"),

            "first_goal_team": first_goal.get("team"),
            "first_goal_minute": first_goal.get("minute"),

            "sub_scores": misc.get("subScores", []),
        })

    return merged

async def parse(date: str):
    result = await _load_date(date=date)
    if result:
        sports = result.get("sports")
        competitions = result.get("competitions")
        events = result.get("events")
        event_miscs = result.get("eventMiscs")

        merged_events = _build(
            sports=sports,
            competitions=competitions,
            events=events,
            event_miscs=event_miscs
        )

    return merged_events


if __name__ == "__main__":
    asyncio.run(run())
