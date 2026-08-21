"""Pure predicate evaluation without execution engines."""

from typing import Any

from src.paths import resolve_path


def evaluate(condition: dict[str, Any], context: dict[str, Any]) -> bool:
    """Evaluate a JSON-serialised predicate against the runtime context."""
    op = condition.get("op")
    if not op:
        raise ValueError("Condition missing 'op' key.")

    if op == "eq":
        path_val = resolve_path(context, condition["path"])
        cmp_val = _resolve_value(condition, context)
        if path_val is None or cmp_val is None:
            return path_val == cmp_val
        if type(path_val) is type(cmp_val):
            return path_val == cmp_val
        return str(path_val).strip().lower() == str(cmp_val).strip().lower()


    elif op in ["lt", "lte", "gt", "gte"]:
        path_val = resolve_path(context, condition["path"])
        cmp_val = _resolve_value(condition, context)
        if path_val is None or cmp_val is None:
            return False
        try:
            p_num, c_num = float(path_val), float(cmp_val)
            if op == "lt":  return bool(p_num < c_num)
            if op == "lte": return bool(p_num <= c_num)
            if op == "gt":  return bool(p_num > c_num)
            if op == "gte": return bool(p_num >= c_num)
        except (ValueError, TypeError):
            return False

    elif op == "is_true":
        val = resolve_path(context, condition["path"])
        if isinstance(val, bool):
            return val is True
        if isinstance(val, str):
            return val.strip().lower() in ["true", "y", "yes", "1"]
        return bool(val)

    elif op == "is_false":
        return bool(resolve_path(context, condition["path"]) is False)

    elif op == "not_empty":
        val = resolve_path(context, condition["path"])
        return bool(val)

    elif op == "and":
        subs = condition.get("all") or condition.get("rules") or []
        return all(evaluate(sub, context) for sub in subs)

    elif op == "or":
        subs = condition.get("any") or condition.get("rules") or []
        return any(evaluate(sub, context) for sub in subs)

    elif op == "not":
        sub = condition.get("condition") or condition.get("rule")
        if not sub:
            raise ValueError("Operator 'not' requires a 'condition' or 'rule' key.")
        return not evaluate(sub, context)

    elif op == "before_now":
        # Workaround to keep pure evaluation: context must supply 'now'
        now_ts = resolve_path(context, "__now__")
        target_ts = resolve_path(context, condition["path"])
        if now_ts is None or target_ts is None:
            return False
        return bool(now_ts > target_ts)
    
    elif op == "contains":
        path_val = resolve_path(context, condition["path"])
        cmp_val = _resolve_value(condition, context)
        if path_val is None or cmp_val is None:
            return False
        return str(cmp_val).strip().lower() in str(path_val).strip().lower()

    raise ValueError(f"Unrecognised operator: {op}")


def _resolve_value(condition: dict[str, Any], context: dict[str, Any]) -> Any:
    if "value" in condition:
        return condition["value"]
    if "value_path" in condition:
        return resolve_path(context, condition["value_path"])
    return None