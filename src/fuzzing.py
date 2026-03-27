import re
import unicodedata
from rapidfuzz import fuzz
from unidecode import unidecode

def _normalize_team_name(name: str) -> str:
    if not name:
        return ""

    name = name.lower().strip()
    name = unicodedata.normalize("NFKD", name)
    name = unidecode(name)
    name = "".join(ch for ch in name if not unicodedata.combining(ch))
    name = re.sub(r"\s+", " ", name).strip()

    return name


def _calc_score(a: str, b: str) -> float:
    a_norm = _normalize_team_name(a)
    b_norm = _normalize_team_name(b)

    if not a_norm or not b_norm:
        return 0.0

    return (
        fuzz.ratio(a_norm, b_norm) * 0.20
        + fuzz.partial_ratio(a_norm, b_norm) * 0.30
        + fuzz.token_sort_ratio(a_norm, b_norm) * 0.20
        + fuzz.token_set_ratio(a_norm, b_norm) * 0.30
    )


def _compare_match(src_team1: str, src_team2: str, cand_team1: str, cand_team2: str) -> float:
    direct = (
        _calc_score(src_team1, cand_team1) +
        _calc_score(src_team2, cand_team2)
    ) / 2

    swapped = (
        _calc_score(src_team1, cand_team2) +
        _calc_score(src_team2, cand_team1)
    ) / 2

    return round(max(direct, swapped), 2)


def find_event(incoming_event: dict, candidate_events: list[dict]) -> dict | None:
    best_match = None

    for candidate in candidate_events:
        score = _compare_match(
            incoming_event.get("team1", ""),
            incoming_event.get("team2", ""),
            candidate.get("team1", ""),
            candidate.get("team2", ""),
        )

        if best_match is None or score > best_match["score"]:
            best_match = {
                "incoming_event": incoming_event,
                "candidate_event": candidate,
                "score": score,
            }

    return best_match