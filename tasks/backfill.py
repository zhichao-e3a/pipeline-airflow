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
        "model_data_onset"
    ]


    for c in collections_to_del:

        n_del = await mongo.delete_all_documents(
            coll_name=c
        )

        print(f"{c}: {n_del} documents deleted")

    print("Documents deleted from all collections")

    for i in ['sql', 'raw']:

        for j in ['rec', 'hist']:

            watermark_log = {
                'pipeline_name' : f'{i}_{j}',
                'last_utime' : '2000-01-01 00:00:00',
            }

            await mongo.upsert_documents_hashed(
                [watermark_log],
                coll_name='watermarks'
            )

    print("Watermarks updated for all colections")