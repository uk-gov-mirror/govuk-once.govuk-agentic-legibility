"""Client helpers to manage workflow execution."""
from typing import Any
from temporalio.client import Client

from src.context import InputSubmission


async def start_run(client: Client, workflow_id: str, definition_dict: dict[str, Any]) -> str:
    """Start an FSM workflow execution."""
    from src.interpreter import SFSMInterpreter

    await client.start_workflow(
        SFSMInterpreter.run,
        definition_dict,
        id=workflow_id,
        task_queue="sfsm-queue",
    )
    return workflow_id


async def submit_input(client: Client, workflow_id: str, token: str, value: Any) -> None:
    """Submit input to an awaiting state."""
    handle = client.get_workflow_handle(workflow_id)
    await handle.execute_update("submit_input", InputSubmission(token=token, value=value))


async def query_awaiting(client: Client, workflow_id: str) -> dict[str, Any] | None:
    """Query what input the workflow is waiting for."""
    handle = client.get_workflow_handle(workflow_id)
    return await handle.query("awaiting")