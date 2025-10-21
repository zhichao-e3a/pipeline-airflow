from core.states import PROC_POOL

from database.MongoDBConnector import MongoDBConnector

from utils.model import combine_data, extract_features

import anyio
import asyncio

from itertools import islice
from typing import List, Dict, Any, Iterable

def _chunks(

        seq: List[Dict[str, Any]],
        size: int

) -> Iterable[List[Dict[str, Any]]]:

    it = iter(seq)

    while True:
        block = list(islice(it, size))
        if not block:
            break
        yield block

async def model(

        mongo   : MongoDBConnector,
        origin  : str

) -> None:

    all_patients = await mongo.get_all_documents(
        coll_name="patients_unified",
        query = {
            "type": {
                "$eq": origin
            }
        },
        projection = {
            "_id"           : 0,
            'date_joined'   : 1,
            "mobile"        : 1,
            "age"           : 1,
            "bmi"           : 1,
            "edd"           : 1,
            "had_pregnancy" : 1,
            "had_preterm"   : 1,
            "had_surgery"   : 1,
            "gdm"           : 1,
            "pih"           : 1,
            "delivery_type" : 1,
            "add"           : 1,
            "onset"         : 1,
            "type"          : 1
        }
    )

    measurements = await mongo.get_all_documents(
        coll_name=f"filt_{origin}",
        projection={
            "_id"               : 1,
            "mobile"            : 1,
            "uc"                : 1,
            "fhr"               : 1,
            "fmov"              : 1,
            "gest_age"          : 1,
            "measurement_date"  : 1,
            "start_test_ts"     : 1
        }
    )

    add, onset = await anyio.to_thread.run_sync(
        lambda: combine_data(
            measurements,
            all_patients
        )
    )

    loop    = asyncio.get_running_loop()
    chunk   = 3000
    async def _proc_map(

            records: List[Dict[str, Any]],

    ) -> List[Dict[str, Any]]:

        futures = [
            loop.run_in_executor(PROC_POOL, extract_features, c)
            for c in _chunks(records, chunk)
        ]

        results: List[Dict[str, Any]] = []

        for fut in asyncio.as_completed(futures):
            part = await fut
            if part:
                results.extend(part)

        return results

    try:
        extracted_onset, extracted_add = await asyncio.gather(
            _proc_map(onset),
            _proc_map(add),
        )
    except asyncio.CancelledError:
        raise

    await mongo.upsert_documents_hashed(
        extracted_onset,
        coll_name="model_data_onset"
    )

    await mongo.upsert_documents_hashed(
        extracted_add,
        coll_name="model_data_add",
    )