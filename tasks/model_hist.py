from core.states import PROC_POOL

from database.MongoDBConnector import MongoDBConnector
from database.SQLDBConnector import SQLDBConnector

from utils.model import combine_data, extract_features, bmi_choose_weight_kg

import anyio
import asyncio
import pandas as pd

from itertools import islice
from typing import List, Dict, Any, Iterable

QUERY = """
    SELECT
    uu.name,
    u.mobile,
    uu.age,
    uu.height,
    uu.old_weight,
    uu.expected_born_date AS edd,
    mm.record_type,
    mm.record_answer
    FROM extant_future_user.user AS u
    JOIN extant_future_user.user_detail AS uu ON u.id = uu.uid
    LEFT JOIN extant_future_user.medical_record AS mm ON uu.uid = mm.user_id AND mm.record_type IN (1, 2, 4, 5, 8, 13)
    WHERE
    u.mobile
    IN
    ({mobile_query_str})
"""

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

async def model_hist(

        sql     : SQLDBConnector,
        mongo   : MongoDBConnector,

) -> None:

    print("RETRIEVING MEASUREMENTS FROM `filt_hist`")

    hist_measurements = await mongo.get_all_documents(
        "filt_hist",
        projection={
            "_id": 1,
            "mobile": 1,
            "uc": 1,
            "fhr": 1,
            # "fmov": 1,
            "gest_age": 1,
            "measurement_date": 1,
            "start_test_ts": 1,
            "add": 1
        }
    )

    unique_mobiles  = set([i['mobile'] for i in hist_measurements])
    mobile_str      = ",".join([f"'{i}'" for i in unique_mobiles])

    print("RETRIEVING METADATA FROM MYSQL")

    filt_hist_df = sql.query_to_dataframe(query=QUERY.format(mobile_query_str=mobile_str))
    filt_hist_pivot = filt_hist_df.pivot(
        index=[i for i in filt_hist_df.columns if i not in ['record_type', 'record_answer']],
        columns='record_type',
        values='record_answer'
    ).reset_index()

    hist_metadata = []
    for _, row in filt_hist_pivot.iterrows():
        # 0='0 pregnancies', 1='1 pregnancies', 2='2 pregnancies', 3='>2 pregnancies'
        # Count current pregnancy as well so treat 0 and 1 as same
        preg_count = row[1.0]
        # 0='有', 1='无', 2='未知'
        had_misc = row[2.0]
        gdm = row[4.0]
        pih = row[5.0]
        had_preterm = row[8.0]
        had_surgery = row[13.0]

        bmi = bmi_choose_weight_kg(
            height_cm=row['height'],
            weight_val=row['old_weight']
        )

        record = {
            'mobile'        : row['mobile'],
            'age'           : int(row['age']) if pd.notna(row['age']) else None,
            'bmi'           : bmi if pd.notna(bmi) else None,
            'edd'           : row['edd'].strftime("%Y-%m-%d") if pd.notna(row['edd']) else None,
            'had_pregnancy' : 1 if (preg_count > 1) else 0,
            'had_preterm'   : 1 if had_preterm == 0 else 0,
            'had_surgery'   : 1 if had_surgery == 0 else 0,
            'gdm'           : 1 if gdm == 0 else 0,
            'pih'           : 1 if pih == 0 else 0,
            'delivery_type' : 'natural',
            'onset'         : None,
            # 'add' : row['add'].to_pydatetime().strftime("%Y-%m-%d %H:%M"),
            'type': 'hist'
        }

        hist_metadata.append(record)

    print(f"{len(unique_mobiles)} PATIENTS RETRIEVED FROM MYSQL")

    print(f"{len(hist_measurements)} MEASUREMENTS RETRIEVED FROM 'filt_hist'")

    add, onset = await anyio.to_thread.run_sync(
        lambda: combine_data(
            hist_measurements,
            hist_metadata
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

    if extracted_add:
        await mongo.upsert_documents_hashed(
            extracted_add,
            coll_name="model_data_hist",
        )

        print(f"{len(extracted_add)} RECORDS UPSERTED TO 'model_data_hist'")