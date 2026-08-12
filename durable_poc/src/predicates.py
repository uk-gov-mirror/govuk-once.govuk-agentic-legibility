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
        return bool(path_val == cmp_val)

    elif op == "lt":
        path_val = resolve_path(context, condition["path"])
        cmp_val = _resolve_value(condition, context)
        if path_val is None or cmp_val is None:
            return False
        return bool(path_val < cmp_val)

    elif op == "is_true":
        return bool(resolve_path(context, condition["path"]) is True)

    elif op == "not_empty":
        val = resolve_path(context, condition["path"])
        return bool(val)

    elif op == "and":
        return all(evaluate(sub, context) for sub in condition.get("all", []))

    elif op == "or":
        return any(evaluate(sub, context) for sub in condition.get("any", []))

    elif op == "not":
        return not evaluate(condition["condition"], context)

    elif op == "before_now":
        # Workaround to keep pure evaluation: context must supply 'now'
        now_ts = resolve_path(context, "__now__")
        target_ts = resolve_path(context, condition["path"])
        if now_ts is None or target_ts is None:
            return False
        return bool(now_ts > target_ts)

    raise ValueError(f"Unrecognised operator: {op}")


def _resolve_value(condition: dict[str, Any], context: dict[str, Any]) -> Any:
    if "value" in condition:
        return condition["value"]
    if "value_path" in condition:
        return resolve_path(context, condition["value_path"])
    return None