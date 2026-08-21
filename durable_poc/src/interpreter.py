"""The durable workflow executor loop."""

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
            workflow.logger.error(f"❌ Definition validation failed: {e}")
            raise DefinitionError(f"Invalid workflow definition: {e}") from e

        if initial_state:
            self.state = initial_state
            workflow.logger.info(f"Resuming workflow from initial_state context with frames: {self.state.frames}")
        else:
            entry_process = self.definition.processes[self.definition.entry]
            initial_vars = entry_process.vars.copy()
            self.state.frames.append(StackFrame(
                process_id=self.definition.entry,
                state_id=entry_process.start,
                vars=initial_vars
            ))
            workflow.logger.info(
                f"Started workflow entry process '{self.definition.entry}' at state '{entry_process.start}' "
                f"with initial vars: {initial_vars}"
            )

        max_transcript_length = 100
        min_steps_between_can = 0
        if self.definition.executor.continue_as_new:
            min_steps_between_can = self.definition.executor.continue_as_new.get("min_steps_between", 50)

        steps_this_run = 0

        while self.state.frames:
            if workflow.info().is_continue_as_new_suggested() and steps_this_run >= min_steps_between_can:
                workflow.logger.info("Executing Continue-As-New...")
                workflow.continue_as_new("SFSMInterpreter", args=[definition_dict, self.state])

            self.state.step_counter += 1
            steps_this_run += 1
            frame = self.state.frames[-1]
            process = self.definition.processes[frame.process_id]
            current_state = process.states.get(frame.state_id)

            if not current_state:
                workflow.logger.error(f"❌ State '{frame.state_id}' not found in process '{frame.process_id}'")
                raise DefinitionError(f"State {frame.state_id} not found in {frame.process_id}")

            workflow.logger.info(
                f"[Step {self.state.step_counter}] [{frame.process_id}:{frame.state_id}] ({type(current_state).__name__})"
            )

            # Prepare context for this step
            context = {
                "input": frame.vars.get("input", {}),
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

                timeout_sec = None
                if current_state.timeout:
                    timeout_str = resolve_dict(current_state.timeout["after"], context)
                    timeout_sec = parse_duration(timeout_str).total_seconds()

                # Interpolate variable tags in the prompt template
                prompt_text = interpolate(current_state.prompt, context)

                self._awaiting_input = AwaitingInput(
                    token=token,
                    prompt=prompt_text,
                    schema=current_state.schema_.model_dump(by_alias=True, exclude_none=True),
                    options=options,
                    timeout_seconds=timeout_sec
                )
                self._received_input = None
                self._timeout_triggered = False
                self._input_ready_event.clear()

                workflow.logger.info(f"Awaiting input token='{token}' prompt='{current_state.prompt}'")

                if current_state.timeout:
                    timeout_str = resolve_dict(current_state.timeout["after"], context)
                    if isinstance(timeout_str, str):
                        duration = parse_duration(timeout_str)
                    else:
                        raise DefinitionError("Timeout after must resolve to duration string")

                    workflow.logger.info(f"⏱ Timeout set for {duration} seconds")
                    try:
                        await workflow.wait_condition(
                            lambda: self._input_ready_event.is_set(),
                            timeout=duration
                        )
                    except asyncio.TimeoutError:
                        self._timeout_triggered = True
                        workflow.logger.warn(f"⚠️ Input timed out at state '{frame.state_id}'")

                else:
                    await workflow.wait_condition(lambda: self._input_ready_event.is_set())

                self._awaiting_input = None

                if self._timeout_triggered:
                    if current_state.timeout and "next" in current_state.timeout:
                        workflow.logger.info(f"➡️ Timeout transition to '{current_state.timeout['next']}'")
                        frame.state_id = current_state.timeout["next"]
                        continue
                    raise DefinitionError("Timeout triggered without next route")
                else:
                    workflow.logger.info(
                        f"Input received for '{current_state.assign}': val={self._received_input} (type={type(self._received_input).__name__})"
                    )
                    set_path(frame.vars, current_state.assign, self._received_input)
                    frame.state_id = current_state.next

            elif isinstance(current_state, OutputState):
                if current_state.channel == "transcript" or current_state.also_transcript:
                    msg = current_state.also_transcript or current_state.message or ""
                    msg = interpolate(msg, context)
                    workflow.logger.info(f"Transcript output: '{msg}'")
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
                    workflow.logger.info(f"Sending notification via '{current_state.channel}'")
                    try:
                        await workflow.execute_activity(
                            activities.notify,
                            notify_params,
                            start_to_close_timeout=timedelta(seconds=30),
                            retry_policy=RetryPolicy(maximum_attempts=5)
                        )
                    except Exception as e:
                        workflow.logger.error(f"Notification activity failed: {e}")
                        if current_state.on_error != "continue":
                            raise e
                
                frame.state_id = current_state.next

            elif isinstance(current_state, CallState):
                body = resolve_dict(current_state.body or {}, context)
                headers = current_state.headers or {}
                url = interpolate(current_state.url, context=context)

                call_params = activities.CallParams(
                    method=current_state.method,
                    url=url,
                    headers=headers,
                    body=body,
                    capture=current_state.capture, 
                    service=current_service if (current_service := getattr(current_state, "service", None)) else None
                )

                retry_pol = RetryPolicy(
                    maximum_attempts=3, 
                    non_retryable_error_types=["ValidationError", "ApplicationError"]
                )

                workflow.logger.info(f"🌐 HTTP {current_state.method} -> {url}")
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
                    workflow.logger.error(f"CallState activity error: {e}")
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
                for idx, rule in enumerate(current_state.rules):
                    eval_res = evaluate(rule.when, context)
                    workflow.logger.info(f"Evaluating rule {idx}: when={rule.when} -> {eval_res}")
                    if eval_res:
                        workflow.logger.info(f"Rule {idx} matched. Transitioning to '{rule.next}'")
                        frame.state_id = rule.next
                        matched = True
                        break
                if not matched:
                    workflow.logger.info(f"No rules matched. Fallback to default '{current_state.default}'")
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
                    workflow.logger.info(f"Assigned '{k}' = {frame.vars.get(k)}")
                frame.state_id = current_state.next

            elif isinstance(current_state, InvokeState):
                target_proc = self.definition.processes[current_state.process]
                new_frame = StackFrame(
                    process_id=current_state.process,
                    state_id=target_proc.start,
                    vars=target_proc.vars.copy()
                )
                
                # Resolve sub-process input arguments
                resolved_inputs = {}
                if current_state.input:
                    resolved_inputs = resolve_dict(current_state.input, context)
                    new_frame.vars["input"] = resolved_inputs

                workflow.logger.info(
                    f"Invoking sub-process '{current_state.process}' "
                    f"with inputs: {resolved_inputs} | Frame variables initialized to: {new_frame.vars}"
                )
                self.state.frames.append(new_frame)

            elif isinstance(current_state, WaitState):
                dur_val = resolve_dict(current_state.duration, context)
                workflow.logger.info(f"💤 Sleeping for {dur_val}")
                if isinstance(dur_val, str):
                    await workflow.sleep(parse_duration(dur_val))
                frame.state_id = current_state.next

            elif isinstance(current_state, EndState):
                workflow.logger.info(
                    f"🏁 Reached EndState '{frame.state_id}' in process '{frame.process_id}' "
                    f"(status={current_state.status}, outcome={current_state.outcome}) | Final vars: {frame.vars}"
                )

                # evaluate return values while child frame vars are active in context
                ret_val = None
                if current_state.return_ is not None:
                    ret_val = resolve_dict(current_state.return_, context)

                # pop child frame after evaluation
                self.state.frames.pop()
                if self.state.frames:
                    parent_frame = self.state.frames[-1]
                    parent_state = self.definition.processes[parent_frame.process_id].states[parent_frame.state_id]
                    
                    if isinstance(parent_state, InvokeState):
                        caught = False
                        if parent_state.catch:
                            for c in parent_state.catch:
                                rule_on = c.on if hasattr(c, "on") else c.get("on")
                                rule_next = c.next if hasattr(c, "next") else c.get("next")
                                if rule_on == current_state.outcome or rule_on == "any":
                                    parent_frame.state_id = rule_next
                                    caught = True
                                    break
                        if not caught:
                            if parent_state.assign:
                                set_path(parent_frame.vars, parent_state.assign, ret_val)
                                workflow.logger.info(f"↩️ Returned {ret_val} into parent var '{parent_state.assign}'")
                            parent_frame.state_id = parent_state.next
                else:
                    return {
                        "status": current_state.status,
                        "outcome": current_state.outcome,
                        "return": ret_val
                    }

    @workflow.update
    async def submit_input(self, msg: InputSubmission) -> None:
        workflow.logger.info(f"📩 Input submitted via update: val={msg.value}")
        self._received_input = msg.value
        self._input_ready_event.set()

    @submit_input.validator
    def _validate_input(self, msg: InputSubmission) -> None:
        if not self._awaiting_input:
            workflow.logger.warn("❌ Rejected update: Workflow is not awaiting input")
            raise InputValidationError("Not awaiting input")
        if msg.token != self._awaiting_input.token:
            workflow.logger.warn(f"❌ Rejected update: Token mismatch ({msg.token} != {self._awaiting_input.token})")
            raise InputValidationError(f"Token mismatch. Expected {self._awaiting_input.token}")
        
        schema = self._awaiting_input.schema
        kind = schema.get("kind")
        val = msg.value

        if kind == "boolean" and not isinstance(val, bool):
            raise InputValidationError(f"Expected boolean, received {type(val).__name__}")
        if kind == "string" and not isinstance(val, str):
            raise InputValidationError(f"Expected string, received {type(val).__name__}")
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