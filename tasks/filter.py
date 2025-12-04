from database.MongoDBConnector import MongoDBConnector

from utils.filter import extract_fetal_movement

import anyio

async def filter(

        mongo   : MongoDBConnector,
        origin  : str

) -> None:

    TASK = 1

    print(f"[{TASK:02d}] START RETRIEVING WATERMARK")

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

    print(f"[{TASK:02d}] END RETRIEVING WATERMARK ({last_utime})") ; TASK += 1

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

    print(f"[{TASK:02d}] START STREAMING RECORDS")

    all_added   = 0
    all_skipped = 0
    async for batch in raw_records:

        batch_skipped = 0

        filt_records    = []
        batch_max_utime = batch[-1]["utime"]

        print(f"[B] {len(batch)} BATCH")

        for record in batch:

            # Check if UC/FHR are both >= 20 minutes
            uc_data     = record['uc']
            fhr_data    = record['fhr']
            if len(uc_data) < 60*20 or len(fhr_data) < 60*20:
                batch_skipped += 1
                continue
            else:
                max_len = max(len(uc_data), len(fhr_data))

                while len(uc_data) < max_len:
                    uc_data.append("0")
                while len(fhr_data) < max_len:
                    fhr_data.append("0")

                record['uc'] = uc_data
                record['fhr'] = fhr_data

                fmov_data = await anyio.to_thread.run_sync(
                    lambda: extract_fetal_movement(record['fmov'], record['measurement_date'])
                ) if record['fmov'] else None

                if fmov_data is not None:
                    if len(fmov_data) < max_len:
                        while len(fmov_data) < max_len:
                            fmov_data.append("0")
                    elif len(fmov_data) > max_len:
                        while len(uc_data) < len(fhr_data):
                            uc_data.append("0")
                            fhr_data.append("0")

                record['fmov'] = fmov_data

            # Check if gestational age is present
            gest_age = record['gest_age']
            if gest_age is None:
                batch_skipped += 1
                continue

            filt_records.append(record)

        print(f"[B] {len(filt_records)} RECORDS BUILT")
        print(f"[B] {batch_skipped} RECORDS SKIPPED")

        all_added   += len(filt_records)
        all_skipped += batch_skipped

        if len(filt_records) > 0:

            print("[B] START UPSERTING RECORDS")

            await mongo.upsert_documents_hashed(filt_records, coll_name=f'filt_{origin}')

            print(f"[B] END UPSERTING RECORDS ({len(filt_records)} RECORDS)")

        print("[B] START UPDATING WATERMARK")

        watermark_log = {
            "pipeline_name": f'raw_{origin}',
            "last_utime": batch_max_utime
        }

        # Upsert watermark to MongoDB
        await mongo.upsert_documents_hashed([watermark_log], "watermarks")

        print(f"[B] END UPDATING WATERMARK ({batch_max_utime})")

    print(f"[{TASK:02d}] END STREAMING RECORDS")

    print(f"[{TASK:02d}] {all_added} RECORDS UPSERTED")
    print(f"[{TASK:02d}] {all_skipped} RECORDS SKIPPED")