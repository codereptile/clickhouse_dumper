import re

import clickhouse_connect
import utils

##############################################
# CONFIG
HOST = "clickhouse.giant.agtrading.ru"
PORT = 443
DATABASES_BLACKLIST = [
    'INFORMATION_SCHEMA',
    'default',
    'information_schema',
    'system',
    '.*prod.*',
    '.*test.*',
    '.*common.*'
]
HIDE_EMPTY_DATABASES = True
##############################################

print("Attempting to connect...", end="\t")
client = clickhouse_connect.get_client(host=HOST, port=PORT, username='default', password='')
print("Connected to ClickHouse!\n")

print("Getting databases...", end="\t")
databases = utils.get_databases(client)

databases_info = []
for database in utils.filter_list_blacklist(databases, DATABASES_BLACKLIST):
    databases_info.append(utils.DatabaseInfo(database))
print("Got {} databases!\n".format(len(databases_info)))

for database in databases_info:
    print("Getting info for database {:30}".format(database.database_name), end="\t")
    database.get_info(client)
    print("Done!")

print("\n" + "=" * 100 + "\n")

for database in databases_info:
    database.print(HIDE_EMPTY_DATABASES)
