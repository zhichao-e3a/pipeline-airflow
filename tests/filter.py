from database.MongoDBConnector import MongoDBConnector

from tasks.filter import filter

import asyncio
import argparse

mode    = 'test'
mongo   = MongoDBConnector(mode=mode)

parser  = argparse.ArgumentParser()
parser.add_argument('--origin', required=True, choices=['rec', 'hist'])
origin  = parser.parse_args().origin

print("=============== START FILTER TASK ===============")
asyncio.run(filter(mongo=mongo, origin=origin))
print("=============== END FILTER TASK ===============")