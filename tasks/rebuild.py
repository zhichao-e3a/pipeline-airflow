from database_manager.database.mongo import MongoDBConnector

async def rebuild(mongo: MongoDBConnector):

    for _, c in enumerate(["RAW_RECORDS", "FILT_RECORDS", "MERGED_RECORDS"]):

        n_del = await mongo.delete_all_documents(coll_name=c)

        print(f"[{_+1}][{c}] {n_del} DOCUMENTS DELETED")

    for c in ['SQL', 'RAW_RECORDS']:

        watermark_log = {
            'pipeline_name' : c,
            'last_utime'    : '2000-01-01 00:00:00',
        }

        await mongo.upsert_documents_hashed(
            coll_name='WATERMARKS',
            records=[watermark_log],
            id_fields=["pipeline_name"]
        )

    print(f"[{_+2}][WATERMARKS] UPDATED TO 2000-01-01 00:00:00")