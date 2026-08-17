# Durable FSM Workflow Executor
A deterministic, durable Finite State Machine (FSM) executor built on the Temporal Python SDK.

This project allows the user to define complex, long-running, asynchronous processes entirely in JSON. The Python workflow executor interprets these JSON definitions dynamically without requiring any workflow-specific code. It handles human-in-the-loop interactions, branching logic, sub-processes, durable timers, and external HTTP integrations natively.

## Key Features
- Zero-Code Workflows: Define states, transitions, HTTP calls, and polling loops entirely in a JSON schema.

- Strict Determinism: All predicates and path resolutions are evaluated using structural recursion. No eval(), exec(), or external expression engines are used, ensuring perfect replayability inside the Temporal sandbox.

- Synchronous Input Validation: Human inputs are submitted via Temporal Updates (not Signals), allowing the workflow to synchronously validate payloads against the schema and reject stale or duplicate tokens.

- Sub-process Stack: Sub-processes execute as frames within the same workflow context (rather than Child Workflows), keeping the entire interpreter state perfectly serializable for continue_as_new.

- Activity Boundaries: HTTP payloads are structurally projected inside the activity. Large or unneeded payloads never cross the workflow boundary, preventing workflow history bloat.

## Project Structure
```text
durable_poc/
├── src/
│   ├── model.py         # Pydantic models enforcing the JSON definition schema
│   ├── paths.py         # Dot-path resolution and string interpolation
│   ├── predicates.py    # Pure, deterministic condition evaluator
│   ├── context.py       # Dataclasses for the interpreter state and frame stack
│   ├── interpreter.py   # The core Temporal Workflow loop
│   ├── activities.py    # Temporal activities (HTTP requests, notifications)
│   ├── errors.py        # Error taxonomy (Retryable, Validation, etc.)
│   ├── worker.py        # Temporal worker bootstrap
│   └── client.py        # Client helpers (start, query, update)
├── tests/
│   ├── test_pure.py     # Unit tests for paths and predicates
│   └── test_workflow.py # Integration tests using Temporal's Time-Skipping server
├── stub_server.py       # FastAPI mock backend (Photo upload & DVLA polling)
└── demo.py              # Interactive terminal CLI frontend
```

## Prerequisites
You will need Python 3.12+ and the Temporal CLI installed.

Install Temporal CLI: Follow the official instructions for your OS (e.g., brew install temporal).

Install Python Dependencies:

```Bash
just build
```

Run the tests:
```bash
just check
```
The test suite validates both the pure Python logic (path resolution, predicates) and the Temporal integration. The integration tests use Temporal's WorkflowEnvironment.start_time_skipping(), allowing workflows with multi-day timeouts to execute in milliseconds.

## Running the Interactive Demo
The project includes an interactive terminal demo (demo.py) that executes a complex "Change of Address" workflow. It prompts you for inputs, validates UK postcodes, simulates uploading a photo, and executes a long-running API polling loop.

To run the demo, you will need to open four separate terminal windows.

#### Terminal 1: Temporal Server
Start the local Temporal development server:

```Bash
temporal server start-dev
```

#### Terminal 2: Backend Stub Server
Start the FastAPI server. This acts as the external APIs (Post Office postcode lookup, DVLA photo upload, and DVLA asynchronous polling endpoint).

```Bash
python stub_server.py
```
(Runs on http://localhost:8000)

#### Terminal 3: Temporal Worker
Start the Python Temporal Worker that executes the FSM interpreter and activities.

```Bash
export PYTHONPATH=. 
python -m src.worker
```

#### Terminal 4: The Interactive CLI
Run the frontend script. This script reads workflow.json, dynamically patches the 4-day timeouts down to 10 seconds for demo purposes, starts the workflow, and renders the prompts to your terminal.

```Bash
export PYTHONPATH=.
python demo.py
```

Follow the prompts in this terminal to step through the state machine.

### Available state types:

- `input`: Suspends the workflow and exposes an awaited schema. Resumes when a matching payload is submitted via Update. Supports timeouts.

- `choice`: Evaluates a list of rules (using operators like eq, lt, is_true, not_empty) and branches the workflow.

- `assign`: Mutates the current stack frame's variable context.

- `call`: Dispatches the http_call activity to make an external request. Maps external errors (4xx/5xx) appropriately and uses capture to project only specific fields into the workflow state.

- `invoke`: Pushes a new sub-process onto the stack. Returns data to the calling frame via assign and handles sub-process exceptions via catch.

- `output`: Emits internal transcript messages or fires external notification activities.

- `wait`: Durably sleeps the workflow for an ISO 8601 duration (e.g., PT5M).

- `end`: Terminates the current process frame with a status and return payload.