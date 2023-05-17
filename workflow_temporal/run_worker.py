import asyncio
from temporalio.client import Client
from temporalio.worker import Worker
import argparse

# Import the activity and workflow from our other files
from activities import *
from workflows import *

async def main():
    # Create client connected to server at the given address
    client = await Client.connect("localhost:7233")

    # Run the worker
    worker = Worker(
        client, 
        task_queue="rikolti-task-queue", 
        workflows=[GetFirstPayload, FetchMetadataPage, GetVernacularPageList, MapMetadataPage],
        activities= [
            get_first_payload,
            fetch_metadata_page,
            get_vernacular_page_list,
            map_metadata_page
        ]
    )
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())