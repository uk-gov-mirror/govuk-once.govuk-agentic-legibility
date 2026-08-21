"""Interactive terminal CLI for the FSM workflow."""

import asyncio
import json
from pathlib import Path
import os

from temporalio.client import Client, WorkflowExecutionStatus
from src.context import InputSubmission


async def main() -> None:
    # Connect to standard local Temporal server
    print("Connecting to server...")
    client = await Client.connect("localhost:7233")
    
    # Load your schema (assuming it's saved as workflow.json)
    print("Loading definition...")
    file_path = Path(f"{os.getcwd()}/durable_poc/dvla_coa_adv_schema.json")
    with open(file_path, "r") as f:
        definition = json.load(f)
        
    # Patch long timeouts for human interaction
    # Change 5-minute poll interval to 2 seconds, and 4-day wait to 10 seconds
    print("Patching time intervals...")
    definition["processes"]["finalisation"]["vars"]["poll_interval"] = "PT2S"
    definition["processes"]["finalisation"]["vars"]["reminder_after"] = "PT15S"

    definition.setdefault("vars", {})["env"] = {
        "dvla_base": "http://localhost:8000/app/photo",
        "postoffice_base": "http://localhost:8000/app/postoffice"
    }
    
    print("Starting Change of Address Workflow...")
    
    # Start the workflow
    handle = await client.start_workflow(
        "SFSMInterpreter",
        args=[definition, None],
        id="interactive-demo",
        task_queue="sfsm-queue",
    )

    print("Workflow started...")
    printed_transcript_len = 0
    
    while True:
        # 1. Check if the workflow has finished
        description = await handle.describe()
        if description.status != WorkflowExecutionStatus.RUNNING:
            # Drain remaining transcript entries before exiting
            transcript = await handle.query("transcript")
            if len(transcript) > printed_transcript_len:
                for entry in transcript[printed_transcript_len:]:
                    print(f"\n📩 [Message]: {entry['message']}")

            result = await handle.result()
            print(f"\n✅ Workflow Complete!")
            print(f"Outcome: {result}")
            break

        # 2. Print any new transcript messages
        transcript = await handle.query("transcript")
        if len(transcript) > printed_transcript_len:
            for entry in transcript[printed_transcript_len:]:
                print(f"\n📩 [Message]: {entry['message']}")
            printed_transcript_len = len(transcript)

        # 3. Check if the workflow is waiting for input
        awaiting = await handle.query("awaiting")
        
        if awaiting:
            # Handle object or dict types returned from query
            schema = awaiting.schema if hasattr(awaiting, "schema") else awaiting["schema"]
            prompt = awaiting.prompt if hasattr(awaiting, "prompt") else awaiting["prompt"]
            token = awaiting.token if hasattr(awaiting, "token") else awaiting["token"]
            options = awaiting.options if hasattr(awaiting, "options") else awaiting.get("options")
            timeout_sec = awaiting.timeout_seconds if hasattr(awaiting, "timeout_seconds") else awaiting.get("timeout_seconds")

            kind = schema.get("kind") if isinstance(schema, dict) else getattr(schema, "kind", None)
            val_key = schema.get("value_key") if isinstance(schema, dict) else getattr(schema, "value_key", None)
            label_key = schema.get("label_key") if isinstance(schema, dict) else getattr(schema, "label_key", None)
            
            print(f"\n🔵 {prompt}")
            
            # If it's a select list, render the options
            if kind == "select_one" and options:
                for opt in options:
                    o_val = opt.get(val_key) if isinstance(opt, dict) else getattr(opt, val_key, None)
                    o_lbl = opt.get(label_key) if isinstance(opt, dict) else getattr(opt, label_key, None)
                    print(f"   [{o_val}] {o_lbl}")
            
            # Non-blocking input if workflow provided timeout_seconds, else standard blocking input
            raw_val = None
            if timeout_sec:
                try:
                    raw_val = await asyncio.wait_for(
                        asyncio.to_thread(input, "> "), 
                        timeout=timeout_sec + 0.5
                    )
                except asyncio.TimeoutError:
                    print("\n⌛ Input timed out waiting for user response...")
                    await asyncio.sleep(1)
                    continue
            else:
                raw_val = input("> ")

            val = None

            if kind == "boolean":
                val = raw_val.strip().lower() in ["y", "yes", "true", "1"]
            elif kind == "file_ref":
                path = raw_val.strip()
                content_type = "image/png" if path.lower().endswith(".png") else "image/jpeg"
                val = {
                    "ref": path,
                    "content_type": content_type,
                    "bytes": os.path.getsize(path) if os.path.exists(path) else 1024
                }
            elif kind == "select_one" and options:
                raw_str = raw_val.strip()
                val_key = schema.get("value_key", "id") if isinstance(schema, dict) else getattr(schema, "value_key", "id")
                
                selected_opt = None
                for opt in options:
                    o_val = opt.get(val_key) if isinstance(opt, dict) else getattr(opt, val_key, None)
                    if str(o_val) == raw_str:
                        selected_opt = opt
                        break

                if selected_opt and isinstance(selected_opt, dict):
                    if val_key in ["uprn", "single_line"]:
                        val = selected_opt
                    else:
                        val = selected_opt.get(val_key, raw_str)
                else:
                    val = raw_str
            else:
                val = raw_val.strip()
                
            print("Submitting...")
            try:
                await handle.execute_update(
                    "submit_input", 
                    InputSubmission(token=token, value=val)
                )
            except Exception as e:
                # E.g., validation errors returned synchronously from the update validator
                print(f"❌ Input rejected: {e}")
                
        else:
            # Workflow is busy processing an activity or waiting on a timer
            await asyncio.sleep(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nDemo terminated.")