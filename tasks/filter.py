from utils.filter import extract_fetal_movement
from schemas.records import assert_records_match_schema

import anyio

from database_manager.database.mongo import MongoDBConnector

async def filter(mongo: MongoDBConnector) -> None:

    curr_watermark = await mongo.get_all_documents(
        coll_name="WATERMARKS",
        query={"_id": "RAW_RECORDS"},
        projection = {
            "_id"        : 0,
            "last_utime" : 1
        }
    )

    last_utime = curr_watermark[0]['last_utime']

    print(f"WATERMARK RETRIEVED ({last_utime})")

    raw_records = mongo.stream_all_documents(
        coll_name = "RAW_RECORDS",
        query = {'utime': {'$gt': last_utime}},
        projection = {
            "ctime"     : 0,
            "doc_hash"  : 0
        },
        sort = [
            ("utime", 1),
            ("_id", 1)
        ]
    )

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
                        while len(uc_data) < len(fmov_data):
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

        assert_records_match_schema(filt_records, record_type="FILT")

        if len(filt_records) > 0:

            print("[B] START UPSERTING RECORDS")

            await mongo.upsert_documents_hashed(
                coll_name=f'FILT_RECORDS',
                records=filt_records
            )

            print(f"[B] END UPSERTING RECORDS ({len(filt_records)} RECORDS)")

        print("[B] START UPDATING WATERMARK")

        watermark_log = {
            "pipeline_name": "RAW_RECORDS",
            "last_utime": batch_max_utime
        }

        # Upsert watermark to MongoDB
        await mongo.upsert_documents_hashed(
            coll_name='WATERMARKS',
            records=[watermark_log],
            id_fields=["pipeline_name"]
        )

        print(f"[B] END UPDATING WATERMARK ({batch_max_utime})")

    print(f"{all_added} RECORDS UPSERTED")
    print(f"{all_skipped} RECORDS SKIPPED")