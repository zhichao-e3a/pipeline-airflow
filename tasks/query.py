from database.MongoDBConnector import MongoDBConnector
from database.SQLDBConnector import SQLDBConnector
from database.queries import *

from utils.query import async_process_df, extract_gest_age

import anyio
import pandas as pd
from datetime import datetime

async def query(

        sql     : SQLDBConnector,
        mongo   : MongoDBConnector,
        origin  : str

) -> None:

    # Historical patients
    if origin == "hist":

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

        print("WATERMARK RETRIEVED:", last_utime)

        df = await anyio.to_thread.run_sync(
            lambda: sql.query_to_dataframe(
                query = HISTORICAL.format(
                   last_utime = last_utime
               )
            )
        )

    # Recruited patients
    elif origin == "rec":

        # Query mobile numbers of recruited patients in 'patients_unified' (have given birth)
        recruited_patients = await mongo.get_all_documents(
            coll_name = "patients_unified",
            query = {
                'type' : 'rec',
                'delivery_type' : {
                    '$ne' : None
                }
            },
            projection = {
                '_id'       : 0,
                'mobile'    : 1,
                'edd'       : 1,
                'add'       : 1
            }
        )

        # Query mobile numbers of recruited patients in 'raw_rec' (have given birth)
        recruited_measurements = await mongo.get_all_documents(
            coll_name = "raw_rec",
            projection = {
                '_id'       : 0,
                'mobile'    : 1
            }
        )

        # Mobile numbers of recruited patients in 'raw_rec' (have given birth)
        measurements_mobile = set([i['mobile'] for i in recruited_measurements])

        # Find mobile numbers in 'patients_unified' but not in 'raw_rec'
        new_additions = {} ; query_string_list = []
        for i in recruited_patients:

            mobile = i['mobile']

            if mobile not in measurements_mobile:

                new_additions[mobile] = i

                query_string_list.append(f"'{mobile}'")

        print(f"{len(recruited_patients)} PATIENTS FETCHED FROM 'patients_unified'")
        print(f"{len(measurements_mobile)} PATIENTS FETCHED FROM 'raw_rec'")
        print(f"{len(new_additions)} NEW PATIENTS")

        if len(new_additions) > 0:
            # Get mobile numbers of 'new' recruited patients
            query_string = ",".join(query_string_list)

            df = await anyio.to_thread.run_sync(
                lambda: sql.query_to_dataframe(
                    query = RECRUITED.format(
                        start = "'2025-03-01 00:00:00'",
                        end = f"'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'",
                        numbers = query_string
                    )
                )
            )
        else:
            return

    print(f"{len(df)} MEASUREMENTS FETCHED FROM SQL")

    # UC, FHR, FMov measurements not ordered yet
    uc_results, fhr_results, fmov_results = await async_process_df(df)

    print(f"DOWNLOADED UC, FHR, FMOV")

    # Order UC and FHR measurements
    sorted_uc_list      = sorted(uc_results, key=lambda x: x[0])
    sorted_fhr_list     = sorted(fhr_results, key=lambda x: x[0])
    sorted_fmov_list    = sorted(fmov_results, key=lambda x: x[0])

    record_list = []
    for idx, row in df.iterrows():

        row_id          = row['id']
        mobile          = row['mobile']

        m_date          = datetime.fromtimestamp(int(row['start_ts']))\
            .strftime("%Y-%m-%d %H:%M:%S")

        start_test_ts   = datetime.fromtimestamp(int(row['start_test_ts']))\
            .strftime("%Y-%m-%d %H:%M:%S") if row['start_test_ts'] else None

        # Extract UC, FHR data ; Do not filter by < 20 minutes yet
        uc_data     = sorted_uc_list[idx][1].split("\n")
        fhr_data    = sorted_fhr_list[idx][1].split("\n")

        # Extract raw FMov data
        raw_fmov_data = sorted_fmov_list[idx][1].split("\n") if sorted_fmov_list[idx][1] else None

        # Extract gestational age
        conclusion = row['conclusion'] ; basic_info = row['basic_info']
        gest_age = extract_gest_age(conclusion, basic_info)

        # Build record (gest_age can be NULL, UC/FHR can be < 20 minutes)
        record = {
            '_id'               : row_id,
            'mobile'            : mobile,
            'measurement_date'  : m_date,
            'start_test_ts'     : start_test_ts,
            'uc'                : uc_data,
            'fhr'               : fhr_data,
            'fmov'              : raw_fmov_data,
            'gest_age'          : gest_age
        }

        # Handle EDD, ADD for historical patients
        if origin == 'hist':

            edd = row['expected_born_date'].strftime("%Y-%m-%d %H:%M:%S")

            add = datetime.fromtimestamp(int(row['end_born_ts'])) \
                .strftime("%Y-%m-%d %H:%M:%S")

        # Handle EDD, ADD for recruited patients
        elif origin == 'rec':

            edd = new_additions[mobile]['edd']

            add = new_additions[mobile]['add']

        record['edd'] = edd ; record['add'] = add

        record_list.append(record)

    print(f"{len(record_list)} RECORDS BUILT")

    if len(record_list) > 0:

        # Upsert records to MongoDB
        if origin == 'hist':

            await mongo.upsert_documents_hashed(record_list, coll_name = 'raw_hist')

            # Historical: Update watermark only if there were records fetched
            latest_utime = pd.to_datetime(df["utime"]) \
                .max().strftime("%Y-%m-%d %H:%M:%S")

            watermark_log = {
                "pipeline_name": f'sql_hist',
                "last_utime": latest_utime
            }

            # Upsert watermark to MongoDB
            await mongo.upsert_documents_hashed([watermark_log], "watermarks")

            print(f"WATERMARK UPDATED: {latest_utime}")

        elif origin == 'rec':

            await mongo.upsert_documents_hashed(record_list, coll_name = 'raw_rec')

        print(f"{len(record_list)} RECORDS UPSERTED TO 'raw_{origin}'")