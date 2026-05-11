import json


def parse_json_output(content, fallback):
    if isinstance(content, (dict, list)):
        return content
    if not isinstance(content, str):
        return fallback

    candidates = [content.strip()]
    if "```" in content:
        start = content.find("```json")
        if start != -1:
            start = content.find("\n", start)
            end = content.find("```", start + 1) if start != -1 else -1
            if start != -1 and end != -1:
                candidates.append(content[start + 1:end].strip())

    l_brace, r_brace = content.find("{"), content.rfind("}")
    if l_brace != -1 and r_brace != -1 and r_brace > l_brace:
        candidates.append(content[l_brace:r_brace + 1])

    for text in candidates:
        try:
            return json.loads(text)
        except Exception:
            continue
    return fallback


def summarize_parsed_json(parsed):
    if isinstance(parsed, dict):
        return {
            "json_ok": True,
            "parsed_type": "dict",
            "top_level_key_count": len(parsed),
            "top_level_keys": list(parsed.keys())[:80],
        }
    if isinstance(parsed, list):
        return {
            "json_ok": True,
            "parsed_type": "list",
            "item_count": len(parsed),
        }
    return {
        "json_ok": False,
        "parsed_type": type(parsed).__name__,
    }


def parse_json_for_debug(content):
    sentinel = object()
    parsed = parse_json_output(content, fallback=sentinel)
    if parsed is sentinel:
        return {"json_ok": False, "parsed_type": None}
    return summarize_parsed_json(parsed)
