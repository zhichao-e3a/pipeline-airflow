from utils.query import async_process_df, extract_gest_age
from schemas.records import assert_records_match_schema

import anyio
import pandas as pd
from datetime import datetime

from database_manager.database.mongo import MongoDBConnector
from database_manager.database.mysql import SQLDBConnector
from database_manager.database.queries import *

async def query(sql: SQLDBConnector, mongo: MongoDBConnector, limit: int=None) -> None:

    curr_watermark = await mongo.get_all_documents(
        coll_name="WATERMARKS",
        query={"_id": "SQL"},
        projection={
            "_id": 0,
            "last_utime": 1
        }
    )

    last_utime = curr_watermark[0]['last_utime']

    print(f"[H] WATERMARK RETRIEVED ({last_utime})")

    custom_query = HISTORICAL.format(last_utime=last_utime)

    if limit is not None:
        custom_query += f" LIMIT {limit}"

    hist_df = await anyio.to_thread.run_sync(lambda: sql.query_to_dataframe(query=custom_query))

    # Create 'edd' and 'add' and 'origin' columns

    hist_df["edd"] = (
        pd.to_datetime(hist_df["expected_born_date"])
        .dt.strftime("%Y-%m-%d %H:%M")
    )

    hist_df["add"] = (
        pd.to_datetime(hist_df["end_born_ts"], unit="s", utc=True)
        .dt.tz_convert("Asia/Singapore")
        .dt.strftime("%Y-%m-%d %H:%M")
    )

    hist_df["origin"] = "HIST"

    print(f"[H] QUERIED FROM NAVICAT ({len(hist_df)} MEASUREMENTS)")

    # Query mobile numbers of recruited patients in 'patients_unified'
    recruited_patients = await mongo.get_all_documents(
        coll_name = "patients_unified",
        query = {'type' : 'rec'},
        projection = {
            '_id'       : 0,
            'mobile'    : 1,
            'edd'       : 1,
            'add'       : 1
        }
    )

    recruited_mobiles = [i['mobile'] for i  in recruited_patients]

    recruited_patients_df = pd.DataFrame(recruited_patients)

    print(f"[R] QUERIED FROM `patients_unified` ({len(recruited_mobiles)} PATIENTS)")

    query_string = ",".join(recruited_mobiles)
    custom_query = RECRUITED.format(
        start="'2025-03-01 00:00:00'",
        end=f"'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'",
        numbers=query_string
    )

    if limit is not None:
        custom_query += f" LIMIT {limit}"

    rec_df = await anyio.to_thread.run_sync(lambda: sql.query_to_dataframe(query = custom_query))

    rec_df = rec_df.merge(recruited_patients_df, on='mobile', how='left')

    rec_df["origin"] = "REC"

    print(f"[R] QUERIED FROM NAVICAT ({len(rec_df)} MEASUREMENTS)")

    df = pd.concat([rec_df, hist_df], ignore_index=True)

    df["measurement_date"] = (
        pd.to_datetime(df["start_ts"], unit="s", utc=True)
        .dt.tz_convert("Asia/Singapore")
        .dt.strftime("%Y-%m-%d %H:%M:%S")
    )

    df["start_test_ts"] = (
        pd.to_datetime(df["start_test_ts"], unit="s", utc=True, errors="coerce")
        .dt.tz_convert("Asia/Singapore")
        .dt.strftime("%Y-%m-%d %H:%M:%S")
    )

    df["sql_utime"] = (
        pd.to_datetime(df["utime"], unit="s", utc=True, errors="coerce")
        .dt.tz_convert("Asia/Singapore")
        .dt.strftime("%Y-%m-%d %H:%M:%S")
    )

    # UC, FHR, FMov measurements not ordered yet
    uc_results, fhr_results, fmov_results = await async_process_df(df)

    print(f"[A] DOWNLOADED UC, FHR, FMOV DATA")

    sorted_uc_list = sorted(uc_results, key=lambda x: x[0])
    sorted_fhr_list = sorted(fhr_results, key=lambda x: x[0])
    sorted_fmov_list = sorted(fmov_results, key=lambda x: x[0])

    df["uc_str"]    = [x[1] for x in sorted_uc_list]
    df["fhr_str"]   = [x[1] for x in sorted_fhr_list]
    df["fmov_str"]  = [x[1] for x in sorted_fmov_list]

    df["uc"]    = df["uc_str"].str.split("\n")
    df["fhr"]   = df["fhr_str"].str.split("\n")
    df["fmov"]  = df["fmov_str"].where(df["fmov_str"].astype(bool), None).str.split("\n")

    df["gest_age"] = df.apply(
        lambda r: extract_gest_age(r["conclusion"], r["basic_info"]),
        axis=1
    ).astype("Int64")

    df.rename(columns={"id": "_id"}, inplace=True)

    df.drop(
        columns=[
            "start_ts",
            "contraction_url",
            "hb_baby_url",
            "raw_fetal_url",
            "basic_info",
            "conclusion",
            "expected_born_date",
            "end_born_ts",
            "utime",
            "uc_str",
            "fhr_str",
            "fmov_str"
        ],
        inplace=True,
        errors="ignore"
    )

    records = df.to_dict(orient="records")

    assert_records_match_schema(records, record_type="RAW")

    if len(df) > 0:

        await mongo.upsert_documents_hashed(
            coll_name='RAW_RECORDS',
            records=records
        )

        print(f"[A] UPSERTED TO 'RAW_RECORDS' ({len(records)} RECORDS)")

        latest_utime = pd.to_datetime(df["sql_utime"]).max().strftime("%Y-%m-%d %H:%M:%S")

        watermark_log = {
            "pipeline_name": "SQL",
            "last_utime": latest_utime
        }

        # Upsert watermark to MongoDB
        await mongo.upsert_documents_hashed(
            coll_name='WATERMARKS',
            records=[watermark_log],
            id_fields=["pipeline_name"]
        )

        print(f"[H] UPSERTED WATERMARK ({latest_utime})")