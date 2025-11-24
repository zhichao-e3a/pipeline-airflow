from database.MongoDBConnector import MongoDBConnector

import asyncio

async def backfill(

        mongo: MongoDBConnector

):

    collections_to_del = [
        "raw_rec",
        "raw_hist",
        "filt_rec",
        "filt_hist",
        "model_data_add",
        "model_data_onset",
        "model_data_hist"
    ]


    for c in collections_to_del:

        n_del = await mongo.delete_all_documents(
            coll_name=c
        )

        print(f"{c}: {n_del} documents deleted")

    print("Documents deleted from all collections")

    for c in ['sql_hist', 'raw_hist', 'raw_rec']:

        watermark_log = {
            'pipeline_name' : c,
            'last_utime' : '2000-01-01 00:00:00',
        }

        await mongo.upsert_documents_hashed(
            [watermark_log],
            coll_name='watermarks'
        )

    print("Watermarks updated for all colections")