from database.MongoDBConnector import MongoDBConnector

from utils.filter import extract_fetal_movement

import anyio

async def filter(

        mongo   : MongoDBConnector,
        origin  : str

) -> None:

    curr_watermark = await mongo.get_all_documents(
        coll_name="watermarks",
        query={
            "_id": {
                "$eq": f"raw_{origin}"
            }
        },
        projection = {
            "_id"        : 0,
            "last_utime" : 1
        }
    )

    last_utime = curr_watermark[0]['last_utime']

    print("WATERMARK RETRIEVED", last_utime)

    if origin == 'hist':
        raw_records = mongo.stream_all_documents(
            coll_name = "raw_hist",
            query = {
                'utime': {
                    '$gt': last_utime,
                }
            },
            projection = {
                "ctime"     : 0,
                "doc_hash"  : 0
            },
            sort = [
                ("utime", 1),
                ("_id", 1)
            ]
        )

    elif origin == 'rec':
        raw_records = mongo.stream_all_documents(
            coll_name="raw_rec",
            query={
                'utime': {
                    '$gt': last_utime,
                }
            },
            projection={
                "ctime"     : 0,
                "doc_hash"  : 0
            },
            sort = [
                ("utime", 1),
                ("_id", 1)
            ]
        )

    total_records   = 0
    bad_uc_fhr      = 0
    no_gest_age     = 0
    async for batch in raw_records:

        filt_records    = []
        batch_max_utime = batch[-1]["utime"]

        for record in batch:

            # Check if UC/FHR are both >= 20 minutes
            uc_data     = record['uc']
            fhr_data    = record['fhr']
            if len(uc_data) < 60*20 or len(fhr_data) < 60*20:
                bad_uc_fhr += 1
                continue
            else:
                max_len = max(len(uc_data), len(fhr_data))

                while len(uc_data) < max_len:
                    uc_data.append("0")
                while len(fhr_data) < max_len:
                    fhr_data.append("0")

                fmov_data = await anyio.to_thread.run_sync(
                    lambda: extract_fetal_movement(record['fmov'], record['measurement_date'], max_len)
                ) if record['fmov'] else None

                if fmov_data:
                    if fmov_data[1] > max_len:
                        while len(uc_data) != fmov_data[1] and len(fhr_data) != fmov_data[1]:
                            uc_data.append("0")
                            fhr_data.append("0")

                record['uc']    = uc_data
                record['fhr']   = fhr_data
                record['fmov']  = fmov_data[0] if fmov_data else None

            # Check if gestational age is present
            gest_age = record['gest_age']
            if gest_age is None:
                no_gest_age += 1
                continue

            filt_records.append(record)

        print(f"{len(filt_records)} RECORDS BUILT")

        if len(filt_records) > 0:

            total_records += len(filt_records)

            if origin == 'hist':
                await mongo.upsert_documents_hashed(filt_records, coll_name = 'filt_hist')

            elif origin == 'rec':
                await mongo.upsert_documents_hashed(filt_records, coll_name = 'filt_rec')

            print(f"{len(filt_records)} RECORDS UPSERTED TO 'filt_{origin}'")

        watermark_log = {
            "pipeline_name": f'raw_{origin}',
            "last_utime": batch_max_utime
        }

        # Upsert watermark to MongoDB
        await mongo.upsert_documents_hashed([watermark_log], "watermarks")

