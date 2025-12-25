from tasks.rebuild import backfill
from tasks.query import query
from tasks.filter import filter

import asyncio
import argparse

from config.configs import TEST_MONGO_CONFIG, SQL_CONFIG
from database_manager.database.mongo import MongoDBConnector
from database_manager.database.mysql import SQLDBConnector

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', required=False)
    limit  = int(parser.parse_args().limit) if parser.parse_args().limit else None

    print("=============== START REBUILD COLLECTIONS ===============")
    mongo = MongoDBConnector(cfg=TEST_MONGO_CONFIG)
    asyncio.run(backfill(mongo=mongo))
    print("=============== END REBUILD COLLECTIONS ===============")

    print("=============== START QUERY TASK ===============")
    mongo = MongoDBConnector(cfg=TEST_MONGO_CONFIG)
    sql = SQLDBConnector(cfg=SQL_CONFIG)
    asyncio.run(query(sql=sql, mongo=mongo, limit=limit))
    print("=============== END QUERY TASK ===============")

    print("=============== START FILTER TASK ===============")
    mongo = MongoDBConnector(cfg=TEST_MONGO_CONFIG)
    asyncio.run(filter(mongo=mongo))
    print("=============== END FILTER TASK ===============")

if __name__ == '__main__':
    main()