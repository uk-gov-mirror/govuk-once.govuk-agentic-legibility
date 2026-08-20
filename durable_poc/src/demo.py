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
    file_path = Path(f"{os.getcwd()}/durable_poc/dvla_coa_schema.json")
    with open(file_path, "r") as f:
        definition = json.load(f)
        
    # Patch long timeouts for human interaction
    # Change 5-minute poll interval to 2 seconds, and 4-day wait to 10 seconds
    print("Patching time intervals...")
    definition["processes"]["address_update"]["vars"]["poll_interval"] = "PT2S"
    definition["processes"]["finalisation"]["vars"]["reminder_after"] = "PT10S"

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
            schema = awaiting["schema"]
            kind = schema.get("kind")
            timeout_sec = awaiting.get("timeout_seconds")
            
            print(f"\n🔵 {awaiting['prompt']}")
            
            # If it's a select list, render the options
            if kind == "select_one" and awaiting.get("options"):
                for opt in awaiting["options"]:
                    print(f"   [{opt[schema['value_key']]}] {opt[schema['label_key']]}")
            
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
                    continue
            else:
                raw_val = input("> ")

            val = None
            
            # Coerce boolean inputs
            if kind == "boolean":
                val = raw_val.strip().lower() in ["y", "yes", "true", "1"]
            elif kind == "file_ref":
                path = raw_val.strip()
                
                # Guess content type for the FSM schema requirements
                content_type = "image/png" if path.lower().endswith(".png") else "image/jpeg"
                
                # Create the payload the FSM actually expects
                val = {
                    "ref": path,
                    "content_type": content_type,
                    "bytes": os.path.getsize(path) if os.path.exists(path) else 1024
                }
            elif kind == "select_one" and awaiting.get("options"):
                raw_str = raw_val.strip()
                val_key = schema.get("value_key")
                
                # Find the full dictionary in the options list matching the UPRN
                selected_opt = next(
                    (opt for opt in awaiting["options"] if str(opt.get(val_key)) == raw_str), 
                    None
                )
                
                if selected_opt:
                    val = selected_opt
                else:
                    val = raw_str
            else:
                val = raw_val.strip()
                
            print("Submitting...")
            try:
                await handle.execute_update(
                    "submit_input", 
                    InputSubmission(token=awaiting["token"], value=val)
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