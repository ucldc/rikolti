import asyncio
from dataclasses import dataclass
from datetime import timedelta
from enum import IntEnum
from typing import List

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.worker import Worker

# Activities that will be called by the workflow


@activity.defn
async def fetch_metadata(amount: int) -> str:
    return f"Fetched metadata..."


@activity.defn
async def map_metadata(amount: int) -> str:
    return f"Mapped metadata..."


@activity.defn
async def fetch_content(amount: int) -> str:
    return f"Fetched content..."


@activity.defn
async def index_records(amount: int) -> str:
    return f"Indexed records..."


# We have to make enumerates IntEnum to be JSON serializable
class Fruit(IntEnum):
    APPLE = 1
    BANANA = 2
    CHERRY = 3
    ORANGE = 4


@dataclass
class ShoppingListItem:
    fruit: Fruit
    amount: int


@dataclass
class ShoppingList:
    items: List[ShoppingListItem]


# Basic workflow that logs and invokes different activities based on input
@workflow.defn
class PurchaseFruitsWorkflow:
    @workflow.run
    async def run(self, list: ShoppingList) -> str:
        # Order each thing on the list
        ordered: List[str] = []
        for item in list.items:
            if item.fruit is Fruit.APPLE:
                ordered.append(
                    await workflow.execute_activity(
                        fetch_metadata,
                        item.amount,
                        start_to_close_timeout=timedelta(seconds=5),
                    )
                )
            elif item.fruit is Fruit.BANANA:
                ordered.append(
                    await workflow.execute_activity(
                        map_metadata,
                        item.amount,
                        start_to_close_timeout=timedelta(seconds=5),
                    )
                )
            elif item.fruit is Fruit.CHERRY:
                ordered.append(
                    await workflow.execute_activity(
                        fetch_content,
                        item.amount,
                        start_to_close_timeout=timedelta(seconds=5),
                    )
                )
            elif item.fruit is Fruit.ORANGE:
                ordered.append(
                    await workflow.execute_activity(
                        index_records,
                        item.amount,
                        start_to_close_timeout=timedelta(seconds=5),
                    )
                )
            else:
                raise ValueError(f"Unrecognized fruit: {item.fruit}")
        return "".join(ordered)


async def main():
    # Start client
    client = await Client.connect("localhost:7233")

    # Run a worker for the workflow
    async with Worker(
        client,
        task_queue="hello-activity-choice-task-queue",
        workflows=[PurchaseFruitsWorkflow],
        activities=[fetch_metadata, map_metadata, fetch_content, index_records],
    ):

        # While the worker is running, use the client to run the workflow and
        # print out its result. Note, in many production setups, the client
        # would be in a completely separate process from the worker.
        result = await client.execute_workflow(
            PurchaseFruitsWorkflow.run,
            ShoppingList(
                [
                    ShoppingListItem(Fruit.APPLE, 8),
                    ShoppingListItem(Fruit.BANANA, 5),
                    ShoppingListItem(Fruit.CHERRY, 1),
                    ShoppingListItem(Fruit.ORANGE, 4),
                ]
            ),
            id="hello-activity-choice-workflow-id",
            task_queue="hello-activity-choice-task-queue",
        )
        print(f"Order result: {result}")


if __name__ == "__main__":
    asyncio.run(main())
