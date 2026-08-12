"""Time-skipping integration tests for the workflow executor."""

import json
from pathlib import Path
from typing import Any
import os

import pytest
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker
from temporalio.client import WorkflowUpdateFailedError

from src.activities import CallParams, NotifyParams, activity
from src.context import InputSubmission
from src.errors import InputValidationError
from src.interpreter import SFSMInterpreter


# Provide mock activities that don't hit the network
@activity.defn(name="http_call")
async def mock_http_call(params: CallParams) -> dict[str, Any]:
    if "search" in params.url:
        return {"status": 200, "addresses": [{"uprn": "1", "single_line": "10 Downing St", "postcode": "SW1A 2AA"}]}
    return {"status": 201, "photo_id": "abc-123"}


@activity.defn(name="notify")
async def mock_notify(params: NotifyParams) -> None:
    pass


@pytest.fixture
def fsm_definition() -> dict[str, Any]:
    cwd = Path(os.getcwd())
    with open(cwd / "durable_poc" / "tests"/ "sample_workflow.json") as f:
        return json.loads(f.read())


@pytest.mark.it("completes the sample workflow")
@pytest.mark.asyncio
async def test_workflow_execution_completes(fsm_definition: dict[str, Any]) -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="test-q",
            workflows=[SFSMInterpreter],
            activities=[mock_http_call, mock_notify],
        ):
            handle = await env.client.start_workflow(
                SFSMInterpreter.run,
                fsm_definition,
                id="test-wf",
                task_queue="test-q",
            )

            # Wait for workflow to hit the first input state
            await env.sleep(0.1)

            awaiting = await handle.query("awaiting")
            assert awaiting is not None
            assert awaiting["prompt"] == "What is your name?"
            
            # Submit string for the name
            await handle.execute_update(
                "submit_input", 
                InputSubmission(token=awaiting["token"], value="Alice")
            )
            
            # Wait for the workflow to transition to the second input state
            await env.sleep(0.1)
            
            awaiting = await handle.query("awaiting")
            assert awaiting is not None
            assert awaiting["prompt"] == "Do you want to receive weather alerts?"
            
            # Submit boolean for the subscription
            await handle.execute_update(
                "submit_input", 
                InputSubmission(token=awaiting["token"], value=True)
            )

            # Workflow should now complete and return the projected data
            result = await handle.result()
            assert result["status"] == "success"
            assert result["return"] == {"name": "Alice", "status": "subscribed"}


@pytest.mark.it("rejects stale tokens")
@pytest.mark.asyncio
async def test_update_validator_rejects_stale_token(fsm_definition: dict[str, Any]) -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client, task_queue="test-q", workflows=[SFSMInterpreter], activities=[mock_http_call, mock_notify],
        ):
            handle = await env.client.start_workflow(
                SFSMInterpreter.run, fsm_definition, id="test-wf-2", task_queue="test-q",
            )
            await env.sleep(0.1)
            
            with pytest.raises(WorkflowUpdateFailedError) as excinfo:
                # Provide string "Alice" to match the first schema if it bypassed the token check
                await handle.execute_update("submit_input", InputSubmission(token="wrong_token", value="Alice"))
            
            # The validator error is nested in the 'cause' of the WorkflowUpdateFailedError
            assert "Token mismatch" in str(excinfo.value.cause)