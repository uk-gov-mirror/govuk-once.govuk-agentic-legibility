"""The durable workflow executor loop."""

import logging
import re
from datetime import timedelta
from typing import Any
import asyncio

from temporalio import workflow
from temporalio.common import RetryPolicy

# Inform temporal to allow imports that might otherwise be deemed unsafe
with workflow.unsafe.imports_passed_through():
    from src.model import (
        AssignState, CallState, ChoiceState, EndState, InputState, InvokeState,
        OutputState, SFSMDefinition, WaitState
    )
    from src.context import AwaitingInput, InputSubmission, InterpreterState, StackFrame, TranscriptEntry
    from src.paths import interpolate, parse_duration, resolve_dict, resolve_path, set_path
    from src.predicates import evaluate
    import pydantic_core
    import pydantic_core._pydantic_core
    import pydantic_core.core_schema
    import pydantic
    import src.activities as activities
    from src.errors import DefinitionError, InputValidationError

logger = logging.getLogger(__name__)


@workflow.defn
class SFSMInterpreter:
    def __init__(self) -> None:
        self.state = InterpreterState()
        self.definition: SFSMDefinition | None = None
        
        # State signals
        self._input_ready_event = asyncio.Event()
        self._awaiting_input: AwaitingInput | None = None
        self._received_input: Any | None = None
        self._timeout_triggered: bool = False

    @workflow.run
    async def run(self, definition_dict: dict[str, Any], initial_state: InterpreterState | None = None) -> Any:
        try:
            self.definition = SFSMDefinition.model_validate(definition_dict)
        except pydantic.ValidationError as e:
            raise DefinitionError(f"Invalid workflow definition: {e}") from e

        if initial_state:
            self.state = initial_state
        else:
            entry_process = self.definition.processes[self.definition.entry]
            self.state.frames.append(StackFrame(
                process_id=self.definition.entry,
                state_id=entry_process.start,
                vars=entry_process.vars.copy()
            ))

        max_transcript_length = 100
        min_steps_between_can = 0
        if self.definition.executor.continue_as_new:
            min_steps_between_can = self.definition.executor.continue_as_new.get("min_steps_between", 50)

        steps_this_run = 0

        while self.state.frames:
            if workflow.info().is_continue_as_new_suggested() and steps_this_run >= min_steps_between_can:
                workflow.continue_as_new("SFSMInterpreter", args=[definition_dict, self.state])

            self.state.step_counter += 1
            steps_this_run += 1
            frame = self.state.frames[-1]
            process = self.definition.processes[frame.process_id]
            current_state = process.states.get(frame.state_id)

            if not current_state:
                raise DefinitionError(f"State {frame.state_id} not found in {frame.process_id}")

            # Prepare context for this step
            context = {
                "input": self.state.env,
                "env": self.state.env,
                "workflow_id": workflow.info().workflow_id,
                "step": self.state.step_counter,
                "__now__": workflow.now().isoformat(),
            }
            context.update(frame.vars)

            if isinstance(current_state, InputState):
                token = f"tkn_{self.state.step_counter}"
                
                # Resolve options if required
                options = None
                if current_state.schema_.options_from:
                    options = resolve_path(context, current_state.schema_.options_from)

                self._awaiting_input = AwaitingInput(
                    token=token,
                    prompt=current_state.prompt,
                    schema=current_state.schema_.model_dump(by_alias=True, exclude_none=True),
                    options=options
                )
                self._received_input = None
                self._timeout_triggered = False
                self._input_ready_event.clear()

                if current_state.timeout:
                    timeout_str = resolve_dict(current_state.timeout["after"], context)
                    if isinstance(timeout_str, str):
                        duration = parse_duration(timeout_str)
                    else:
                        raise DefinitionError("Timeout after must resolve to duration string")

                    try:
                        await workflow.wait_condition(
                            lambda: self._input_ready_event.is_set(),
                            timeout=duration
                        )
                    except asyncio.TimeoutError:
                        self._timeout_triggered = True

                else:
                    await workflow.wait_condition(lambda: self._input_ready_event.is_set())

                self._awaiting_input = None

                if self._timeout_triggered:
                    if current_state.timeout and "next" in current_state.timeout:
                        frame.state_id = current_state.timeout["next"]
                        continue
                    raise DefinitionError("Timeout triggered without next route")
                else:
                    set_path(frame.vars, current_state.assign, self._received_input)
                    frame.state_id = current_state.next

            elif isinstance(current_state, OutputState):
                if current_state.channel == "transcript" or current_state.also_transcript:
                    msg = current_state.also_transcript or current_state.message or ""
                    msg = interpolate(msg, context)
                    self.state.transcript.append(TranscriptEntry(
                        step=self.state.step_counter,
                        timestamp=workflow.now().isoformat(),
                        message=msg
                    ))
                    if len(self.state.transcript) > max_transcript_length:
                        self.state.transcript = self.state.transcript[-max_transcript_length:]
                        # TODO: offload older entries to an activity

                if current_state.channel != "transcript":
                    notify_params = activities.NotifyParams(
                        channel=current_state.channel,
                        template=current_state.template or "",
                        params=resolve_dict(current_state.params or {}, context)
                    )
                    try:
                        await workflow.execute_activity(
                            activities.notify,
                            notify_params,
                            start_to_close_timeout=timedelta(seconds=30),
                            retry_policy=RetryPolicy(maximum_attempts=5)
                        )
                    except Exception as e:
                        if current_state.on_error != "continue":
                            raise e
                
                frame.state_id = current_state.next

            elif isinstance(current_state, CallState):
                body = resolve_dict(current_state.body or {}, context)
                headers = current_state.headers or {}

                call_params = activities.CallParams(
                    method=current_state.method,
                    url=current_state.url,
                    headers=headers,
                    body=body,
                    capture=current_state.capture, 
                    service=current_state.service
                )

                retry_pol = RetryPolicy(
                    maximum_attempts=3, 
                    non_retryable_error_types=["ValidationError", "ApplicationError"]
                )

                try:
                    result = await workflow.execute_activity(
                        activities.http_call,
                        call_params,
                        start_to_close_timeout=timedelta(seconds=30),
                        retry_policy=retry_pol
                    )
                    set_path(frame.vars, current_state.assign, result)
                    frame.state_id = current_state.next
                except Exception as e:
                    # simplistic catch routing
                    handled = False
                    if current_state.catch:
                        for c in current_state.catch:
                            if c["on"] == "any":
                                frame.state_id = c["next"]
                                handled = True
                                break
                    if not handled:
                        raise e

            elif isinstance(current_state, ChoiceState):
                matched = False
                for rule in current_state.rules:
                    if evaluate(rule.when, context):
                        frame.state_id = rule.next
                        matched = True
                        break
                if not matched:
                    frame.state_id = current_state.default

            elif isinstance(current_state, AssignState):
                for k, v in current_state.set.items():
                    if isinstance(v, dict) and "op" in v:
                        if v["op"] == "add":
                            val1 = resolve_path(context, v["path"])
                            val2 = resolve_path(context, v.get("value_path", "")) if "value_path" in v else v.get("value")
                            set_path(frame.vars, k, val1 + val2)
                        elif v["op"] == "now_plus":
                            dur_str = resolve_path(context, v["value_path"])
                            if dur_str:
                                dt = workflow.now() + parse_duration(dur_str)
                                set_path(frame.vars, k, dt.isoformat())
                    else:
                        set_path(frame.vars, k, resolve_dict(v, context))
                frame.state_id = current_state.next

            elif isinstance(current_state, InvokeState):
                target_proc = self.definition.processes[current_state.process]
                new_frame = StackFrame(
                    process_id=current_state.process,
                    state_id=target_proc.start,
                    vars=target_proc.vars.copy()
                )
                if current_state.input:
                    new_frame.vars["input"] = resolve_dict(current_state.input, context)
                self.state.frames.append(new_frame)

            elif isinstance(current_state, WaitState):
                dur_val = resolve_dict(current_state.duration, context)
                if isinstance(dur_val, str):
                    await workflow.sleep(parse_duration(dur_val))
                frame.state_id = current_state.next

            elif isinstance(current_state, EndState):
                self.state.frames.pop()
                if self.state.frames:
                    parent_frame = self.state.frames[-1]
                    parent_state = self.definition.processes[parent_frame.process_id].states[parent_frame.state_id]
                    
                    if isinstance(parent_state, InvokeState):
                        if current_state.outcome and parent_state.catch:
                            caught = False
                            for c in parent_state.catch:
                                if c.on == current_state.outcome or c.on == "any":
                                    parent_frame.state_id = c.next
                                    caught = True
                                    break
                            if not caught:
                                parent_frame.state_id = parent_state.next
                        else:
                            if parent_state.assign:
                                ret_val = resolve_dict(current_state.return_, context)
                                set_path(parent_frame.vars, parent_state.assign, ret_val)
                            parent_frame.state_id = parent_state.next
                else:
                    return {
                        "status": current_state.status,
                        "outcome": current_state.outcome,
                        "return": resolve_dict(current_state.return_, context)
                    }

    @workflow.update
    async def submit_input(self, msg: InputSubmission) -> None:
        self._received_input = msg.value
        self._input_ready_event.set()

    @submit_input.validator
    def _validate_input(self, msg: InputSubmission) -> None:
        if not self._awaiting_input:
            raise InputValidationError("Not awaiting input")
        if msg.token != self._awaiting_input.token:
            raise InputValidationError(f"Token mismatch. Expected {self._awaiting_input.token}")
        
        schema = self._awaiting_input.schema
        kind = schema.get("kind")
        val = msg.value

        if kind == "boolean" and not isinstance(val, bool):
            raise InputValidationError("Expected boolean")
        if kind == "string" and not isinstance(val, str):
            raise InputValidationError("Expected string")
        if kind == "string" and "pattern" in schema:
            if not re.match(str(schema["pattern"]), str(val)):
                raise InputValidationError(schema.get("invalid_message", "Invalid format"))
        # Further schema validations (enum, object, etc.) would be handled similarly here

    @workflow.query
    def awaiting(self) -> AwaitingInput | None:
        return self._awaiting_input

    @workflow.query
    def transcript(self) -> list[TranscriptEntry]:
        return self.state.transcript