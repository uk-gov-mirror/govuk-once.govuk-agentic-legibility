"""Worker bootstrap."""

import asyncio
import logging

from temporalio.client import Client
from temporalio.worker import Worker

from src.activities import http_call, notify
from src.interpreter import SFSMInterpreter

logging.basicConfig(level=logging.INFO)


async def main() -> None:
    client = await Client.connect("localhost:7233")
    worker = Worker(
        client,
        task_queue="sfsm-queue",
        workflows=[SFSMInterpreter],
        activities=[http_call, notify],
    )
    logging.info("Starting worker...")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())