"""Activities bridging the workflow to external services."""

import logging
from typing import Any
from dataclasses import dataclass

import httpx
from temporalio import activity

from src.errors import RetryableHttpError

logger = logging.getLogger(__name__)


@dataclass
class CallParams:
    method: str
    url: str
    headers: dict[str, str] | None
    body: dict[str, Any] | None
    capture: dict[str, Any]


@activity.defn
async def http_call(params: CallParams) -> dict[str, Any]:
    """Execute an HTTP call and return a projected snapshot."""
    async with httpx.AsyncClient() as client:
        try:
            response = await client.request(
                method=params.method,
                url=params.url,
                headers=params.headers,
                json=params.body,
                timeout=15.0,
            )
        except httpx.RequestError as e:
            raise RetryableHttpError(f"HTTP request failed: {e}") from e

    # 5xx and 429 are retryable
    if response.status_code >= 500 or response.status_code == 429:
        raise RetryableHttpError(f"HTTP {response.status_code}")

    body = {}
    if response.headers.get("Content-Type", "").startswith("application/json"):
        try:
            body = response.json()
        except Exception:
            pass

    full_context = {
        "status": response.status_code,
        "headers": dict(response.headers),
        "body": body,
    }

    return _project_capture(full_context, params.capture)


def _project_capture(full_context: dict[str, Any], capture_spec: dict[str, Any]) -> dict[str, Any]:
    """Apply the capture specification to the raw HTTP context."""
    from src.paths import resolve_path  # Local import to prevent circularity

    result: dict[str, Any] = {}
    for key, spec in capture_spec.items():
        if isinstance(spec, str):
            result[key] = resolve_path(full_context, spec)
        elif isinstance(spec, dict):
            source_list = resolve_path(full_context, spec.get("from", ""))
            
            if isinstance(source_list, list):
                picks = spec.get("pick")
                max_items = spec.get("max_items")
                projected = []
                for item in source_list:
                    if isinstance(item, dict) and picks:
                        projected.append({p: item.get(p) for p in picks})
                    else:
                        projected.append(item)
                if max_items:
                    projected = projected[:max_items]
                result[key] = projected
            elif isinstance(source_list, str) and "max_length" in spec:
                result[key] = source_list[: spec["max_length"]]
            else:
                result[key] = source_list
    return result


@dataclass
class NotifyParams:
    channel: str
    template: str
    params: dict[str, Any]


@activity.defn
async def notify(params: NotifyParams) -> None:
    """Mock notification activity."""
    logger.info(f"Notifying via {params.channel} using {params.template}: {params.params}")