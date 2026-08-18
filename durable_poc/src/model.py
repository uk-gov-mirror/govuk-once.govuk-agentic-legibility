"""Pydantic models for the FSM schema."""

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class SchemaDefinition(BaseModel):
    model_config = ConfigDict(extra="allow")
    kind: str
    pattern: str | None = None
    normalise: str | None = None
    max_length: int | None = None
    invalid_message: str | None = None
    fields: list[dict[str, Any]] | None = None
    options_from: str | None = None
    value_key: str | None = None
    label_key: str | None = None


class BaseState(BaseModel):
    type: str


class InputState(BaseState):
    type: Literal["input"] = "input"
    prompt: str
    schema_: SchemaDefinition = Field(alias="schema")
    assign: str
    on_invalid: str | None = None
    timeout: dict[str, Any] | None = None
    next: str


class OutputState(BaseState):
    type: Literal["output"] = "output"
    channel: str
    message: str | None = None
    template: str | None = None
    params: dict[str, Any] | None = None
    idempotency_key: str | None = None
    also_transcript: str | None = None
    on_error: Literal["continue", "fail"] = "fail"
    next: str


class CallState(BaseState):
    type: Literal["call"] = "call"
    method: str
    url: str
    service: str
    headers: dict[str, str] | None = None
    body: dict[str, Any] | None = None
    source_ref: dict[str, Any] | None = None
    encoding: str | None = None
    idempotency_key: str | None = None
    timeouts: dict[str, str] | None = None
    retry: dict[str, Any] | None = None
    assign: str
    capture: dict[str, Any]
    catch: list[dict[str, str]] | None = None
    next: str


class ChoiceRule(BaseModel):
    when: dict[str, Any]
    next: str


class ChoiceState(BaseState):
    type: Literal["choice"] = "choice"
    rules: list[ChoiceRule]
    default: str


class AssignState(BaseState):
    type: Literal["assign"] = "assign"
    set: dict[str, Any]
    next: str


class InvokeCatch(BaseModel):
    on: str
    next: str


class InvokeState(BaseState):
    type: Literal["invoke"] = "invoke"
    process: str
    assign: str | None = None
    input: dict[str, Any] | None = None
    catch: list[InvokeCatch] | None = None
    next: str


class WaitState(BaseState):
    type: Literal["wait"] = "wait"
    duration: str | dict[str, str]
    next: str


class EndState(BaseState):
    type: Literal["end"] = "end"
    status: str
    outcome: str | None = None
    return_: Any | None = Field(default=None, alias="return")


State = InputState | OutputState | CallState | ChoiceState | AssignState | InvokeState | WaitState | EndState


class Process(BaseModel):
    start: str
    vars: dict[str, Any] = Field(default_factory=dict)
    states: dict[str, State]


class WorkflowExecutorConfig(BaseModel):
    workflow_id_template: str | None = None
    id_reuse_policy: str | None = None
    run_timeout: str | None = None
    continue_as_new: dict[str, Any] | None = None
    search_attributes: dict[str, Any] | None = None


class SFSMDefinition(BaseModel):
    schema_: str = Field(alias="schema")
    id: str
    version: str
    entry: str
    executor: WorkflowExecutorConfig
    defaults: dict[str, Any] = Field(default_factory=dict)
    processes: dict[str, Process]