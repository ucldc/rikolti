from datetime import timedelta
from temporalio import workflow

# Import our activity, passing it through the sandbox
with workflow.unsafe.imports_passed_through():
    from activities import *

@workflow.defn
class GetFirstPayload:
    @workflow.run
    async def run(self, collection_id: str) -> str:
        result = await workflow.execute_activity(
            get_first_payload, collection_id, schedule_to_close_timeout=timedelta(seconds=5)
        )

        return result

@workflow.defn
class FetchMetadataPage:
    @workflow.run
    async def run(self, payload: str) -> str:
        result = await workflow.execute_activity(
            fetch_metadata_page, payload, schedule_to_close_timeout=timedelta(seconds=5)
        )

        return result

@workflow.defn
class GetVernacularPageList:
    @workflow.run
    async def run(self, collection_id: str) -> list:
        result = await workflow.execute_activity(
            get_vernacular_page_list, collection_id, schedule_to_close_timeout=timedelta(seconds=5)
        )

        return result

@workflow.defn
class MapMetadataPage:
    @workflow.run
    async def run(self, payload: dict) -> dict:
        result = await workflow.execute_activity(
            map_metadata_page, payload, schedule_to_close_timeout=timedelta(seconds=5)
        )

        return result
