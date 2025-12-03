from database.MongoDBConnector import MongoDBConnector
from database.SQLDBConnector import SQLDBConnector

from tasks.backfill import backfill
from tasks.query import query

import asyncio
import argparse

mode    = 'test'
sql     = SQLDBConnector()

parser  = argparse.ArgumentParser()
parser.add_argument('--origin', required=True, choices=['rec', 'hist'])
parser.add_argument('--limit', required=True, type=int)
origin  = parser.parse_args().origin
limit   = parser.parse_args().limit

print("=============== START CLEAR COLLECTIONS ===============")
mongo = MongoDBConnector(mode=mode)
asyncio.run(backfill(mongo=mongo))
print("=============== END CLEAR COLLECTIONS ===============")

print("=============== START QUERY TASK ===============")
mongo = MongoDBConnector(mode=mode)
asyncio.run(query(sql=sql, mongo=mongo, origin=origin, limit=limit))
print("=============== END QUERY TASK ===============")