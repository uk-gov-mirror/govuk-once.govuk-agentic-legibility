"""Runtime context and state persistence models."""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class StackFrame:
    process_id: str
    state_id: str
    vars: dict[str, Any]


@dataclass
class TranscriptEntry:
    step: int
    timestamp: str
    message: str


@dataclass
class InterpreterState:
    frames: list[StackFrame] = field(default_factory=list)
    transcript: list[TranscriptEntry] = field(default_factory=list)
    step_counter: int = 0
    env: dict[str, Any] = field(default_factory=dict)


@dataclass
class AwaitingInput:
    token: str
    prompt: str
    schema: dict[str, Any]
    options: Any | None = None


@dataclass
class InputSubmission:
    token: str
    value: Any