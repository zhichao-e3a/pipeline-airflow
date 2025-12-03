from database.MongoDBConnector import MongoDBConnector
from database.SQLDBConnector import SQLDBConnector
from database.queries import *

from tasks.backfill import backfill

from utils.query import async_process_df, extract_gest_age

import anyio
import asyncio
import argparse
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo

async def query() -> None:

    TASK = 1

    mode    = 'test'
    sql     = SQLDBConnector()
    mongo   = MongoDBConnector(mode=mode)

    parser = argparse.ArgumentParser()
    parser.add_argument('--origin', required=True, choices=['rec', 'hist'])
    parser.add_argument('--limit', required=True, type=int)
    origin  = parser.parse_args().origin
    limit   = parser.parse_args().limit

    print("=============== START CLEAR COLLECTIONS ===============")
    await backfill(mongo=mongo)
    print("=============== END CLEAR COLLECTIONS ===============")

    print("=============== START QUERY TASK ===============")
    # Historical patients
    if origin == "hist":

        print(f"[{TASK:02d}] START RETRIEVING WATERMARK")
        curr_watermark = await mongo.get_all_documents(
            coll_name="watermarks",
            query={
                "_id": {
                    "$eq": f"sql_hist"
                }
            },
            projection={
                "_id": 0,
                "last_utime": 1
            }
        )
        last_utime = curr_watermark[0]['last_utime']
        print(f"[{TASK:02d}] END RETRIEVING WATERMARK ({last_utime})")
        TASK += 1

        print(f"[{TASK:02d}] START QUERYING FROM NAVICAT")
        limit_query = f"{HISTORICAL.format(last_utime=last_utime)} LIMIT {limit}"
        df = await anyio.to_thread.run_sync(
            lambda: sql.query_to_dataframe(
                query=limit_query
            )
        )
        print(f"[{TASK:02d}] END QUERYING FROM NAVICAT ({len(df)})")
        TASK += 1

    # Recruited patients
    elif origin == "rec":

        print(f"[{TASK:02d}] START RETRIEVING PATIENTS")
        recruited_patients = await mongo.get_all_documents(
            coll_name="patients_unified",
            query={
                'type': 'rec',
                'delivery_type': {
                    '$ne': None
                }
            },
            projection={
                '_id': 0,
                'mobile': 1,
                'edd': 1,
                'add': 1
            }
        )
        print(f"[{TASK:02d}] END RETRIEVING PATIENTS ({len(recruited_patients)})")
        TASK += 1

        print(f"[{TASK:02d}] START RETRIEVING MEASUREMENTS")
        recruited_measurements = await mongo.get_all_documents(
            coll_name="raw_rec",
            projection={
                '_id': 0,
                'mobile': 1
            }
        )
        print(f"[{TASK:02d}] END RETRIEVING MEASUREMENTS ({len(recruited_measurements)})")
        TASK += 1

        print(f"[{TASK:02d}] START IDENTIFYING NEW PATIENTS")
        measurements_mobile = set([i['mobile'] for i in recruited_measurements])
        new_additions = {} ; query_string_list = []
        for i in recruited_patients:
            mobile = i['mobile']
            if mobile not in measurements_mobile:
                new_additions[mobile] = i
                query_string_list.append(f"'{mobile}'")
        print(f"[{TASK:02d}] END IDENTIFYING NEW PATIENTS ({len(new_additions)})")
        TASK += 1

        if len(new_additions) > 0:
            print(f"[{TASK:02d}] START QUERYING FROM NAVICAT")
            query_string = ",".join(query_string_list)
            limit_query=f"{RECRUITED.format(start="'2025-03-01 00:00:00'",end=f"'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'",numbers=query_string)} LIMIT {limit}"
            df = await anyio.to_thread.run_sync(
                lambda: sql.query_to_dataframe(
                    query=limit_query
                )
            )
            print(f"[{TASK:02d}] END QUERYING FROM NAVICAT")
            TASK += 1

    # UC, FHR, FMov measurements not ordered yet
    print(f"[{TASK:02d}] START DOWNLOADING UC, FHR, FMOV")
    uc_results, fhr_results, fmov_results = await async_process_df(df)
    print(f"[{TASK:02d}] END DOWNLOADING UC, FHR, FMOV")

    # Order UC and FHR measurements
    sorted_uc_list      = sorted(uc_results, key=lambda x: x[0])
    sorted_fhr_list     = sorted(fhr_results, key=lambda x: x[0])
    sorted_fmov_list    = sorted(fmov_results, key=lambda x: x[0])

    print(f"[{TASK:02d}] START BUILDING RECORDS")
    record_list = []
    for idx, row in df.iterrows():
        row_id = row['id']
        mobile = row['mobile']
        m_date = datetime.fromtimestamp(
            int(row['start_ts']),
            tz=ZoneInfo("Asia/Singapore")
        ).strftime("%Y-%m-%d %H:%M:%S")
        start_test_ts = datetime.fromtimestamp(
            int(row['start_test_ts']),
            tz=ZoneInfo("Asia/Singapore")
        ).strftime("%Y-%m-%d %H:%M:%S") if row['start_test_ts'] else None
        uc_data         = sorted_uc_list[idx][1].split("\n")
        fhr_data        = sorted_fhr_list[idx][1].split("\n")
        raw_fmov_data   = sorted_fmov_list[idx][1].split("\n") if sorted_fmov_list[idx][1] else None
        conclusion      = row['conclusion'] ; basic_info = row['basic_info']
        gest_age        = extract_gest_age(conclusion, basic_info)
        record = {
            '_id': row_id,
            'mobile': mobile,
            'measurement_date'  : m_date,
            'start_test_ts': start_test_ts,
            'uc': uc_data,
            'fhr': fhr_data,
            'fmov'              : raw_fmov_data,
            'gest_age'  : gest_age
        }

        if origin == 'hist':
            edd = row['expected_born_date'].strftime("%Y-%m-%d")
            add = datetime.fromtimestamp(
                int(row['end_born_ts']),
                tz=ZoneInfo("Asia/Singapore")
            ).strftime("%Y-%m-%d %H:%M")

        elif origin == 'rec':
            edd = new_additions[mobile]['edd']
            add = new_additions[mobile]['add']
        record['edd'] = edd ; record['add'] = add
        record_list.append(record)

    print(f"[{TASK:02d}] END BUILDING RECORDS ({len(record_list)})")
    TASK += 1

    if len(record_list) > 0:
        if origin == 'hist':
            print(f"[{TASK:02d}] START UPSERTING RECORDS")
            await mongo.upsert_documents_hashed(record_list, coll_name='raw_hist')
            print(f"[{TASK:02d}] END UPSERTING RECORDS ({len(record_list)})")
            TASK += 1

            print(f"[{TASK:02d}] START UPDATING WATERMARK")
            latest_utime = pd.to_datetime(df["utime"]) \
                .max().strftime("%Y-%m-%d %H:%M:%S")
            watermark_log = {
                "pipeline_name": 'sql_hist',
                "last_utime": latest_utime
            }
            await mongo.upsert_documents_hashed([watermark_log], "watermarks")
            print(f"[{TASK:02d}] END UPDATING WATERMARK ({latest_utime})")
            TASK += 1

        elif origin == 'rec':
            print(f"[{TASK:02d}] START UPSERTING RECORDS")
            await mongo.upsert_documents_hashed(record_list, coll_name='raw_rec')
            print(f"[{TASK:02d}] END UPSERTING RECORDS ({len(record_list)})")

    print("=============== END QUERY TASK ===============")

if __name__ == "__main__":
    asyncio.run(query())