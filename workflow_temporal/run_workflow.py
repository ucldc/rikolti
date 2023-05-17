import asyncio
import argparse
from temporalio.client import Client
import json
import requests

# Import the workflow from the previous code
from workflows import *

async def main(collection_id: str, tasks: str) -> str:
    # Create client connected to server at the given address
    client = await Client.connect("localhost:7233")

    # idea is that the calling program could pass in an array 
    # of tasks to be run 
    task_map = {
        '1': 'Fetch metadata',
        '2': 'Map metadata',
        '3': 'validate_mapping',
        '4': 'fetch_content',
        '5': 'index_records'
    }

    if tasks == 'all':
        run_tasks = [key for key in task_map]
    else:
        tasks = [tasks]
        run_tasks = [key for key in task_map if key in tasks]

    if '1' in run_tasks:
        print(f"{task_map['1']} for collection {collection_id}")

        # get the first fetcher payload
        payload = await client.execute_workflow(
            GetFirstPayload.run,
            collection_id,
            id=f"get-first-payload-{collection_id}",
            task_queue="rikolti-task-queue"
        )

        # fetch the metadata page by page, one after the other
        await fetch_collection_metadata(payload, collection_id, 0)


    if '2' in run_tasks:
        print(f"{task_map['2']} for collection {collection_id}")
        
        # get a list of metadata files (lambda_shepherd.py)
        page_list = await client.execute_workflow(
            GetVernacularPageList.run,
            collection_id,
            id=f"get-vernacular-page-list-{collection_id}",
            task_queue="rikolti-task-queue"
        )
        print(f"[Page List] {page_list}")

        payload = {"collection_id": collection_id}
        collection = requests.get(
            f'https://registry.cdlib.org/api/v1/'
            f'rikolticollection/{collection_id}/?format=json').json()
        payload.update({'collection': collection})

        # We want to map some number of pages of metadata in parallel.
        # Could use the child workflow approach or the parallel approach as
        # shown in samples here: https://github.com/temporalio/samples-python
        # We could AWS lambda for fan-out? Would need to figure out fan-in
        for page in page_list:
            payload.update({'page_filename': page})
            await client.execute_workflow(
                MapMetadataPage.run,
                payload,
                id=f"map-metadata-page-{collection_id}-{page}",
                task_queue='rikolti-task-queue'
            )


async def fetch_collection_metadata(payload: str, collection_id: str, page_id: int) -> str:
    # FIXME pass client around?
    client = await Client.connect("localhost:7233")

    next_page = await client.execute_workflow(
        FetchMetadataPage.run,
        payload,
        id=f"fetch-md-{collection_id}-{page_id}",
        task_queue="rikolti-task-queue"
    )

    if not json.loads(next_page).get('finished'):
        page_id = page_id + 1
        await fetch_collection_metadata(next_page, collection_id, page_id)

    return next_page

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('collection_id')
    parser.add_argument('--tasks', type=str, default='all')
    args = parser.parse_args()
    asyncio.run(main(args.collection_id, args.tasks))