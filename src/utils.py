def extract_by_schema(data: dict, schema: dict, map_key: str = None):
    """ 
    Extract given in schema fields from object

    *schema: Selects an occerrence of itself from an object
    *map_key: If passed, hashmap the object ** mayn't be in schema
    """
    def process(item, schema):
        result = {}

        for key, rule in schema.items():
            value = item.get(key)

            if value is None:
                continue

            if rule is True:
                result[key] = value

            elif isinstance(rule, dict) and isinstance(value, dict):
                result[key] = process(value, rule)

            elif isinstance(rule, list) and isinstance(value, list):
                sub_schema = rule[0]
                result[key] = [process(v, sub_schema) for v in value]

        return result

    if isinstance(data, list):
        if map_key:
            result = {}
            for item in data:
                processed = process(item, schema)

                key = processed.get(map_key) or item.get(map_key)

                if key is None:
                    continue

                processed.pop(map_key, None)

                result[key] = processed

            return result

        return [process(item, schema) for item in data]

    return process(data, schema)


def resolve_competition(event, id_to_event, id_to_competition):
    """ Returning competition depends on hierarchy (parent/child) """
    competition_id = event.get("competitionId")

    if not competition_id and event.get("parent_id"):
        parent = id_to_event.get(event["parent_id"])
        if parent:
            competition_id = parent.get("competitionId")

    if competition_id:
        return id_to_competition.get(competition_id)

    return None
