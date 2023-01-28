import clickhouse_connect
import utils

##############################################
# CONFIG
DATABASE = "database_name"  # DOUBLE CHECK THIS!!!
DATE_UNTIL = 'yyyy-mm-dd'  # DOUBLE CHECK THIS!!!

HOST = "clickhouse.giant.agtrading.ru"
PORT = 443
##############################################

print("Attempting to connect...", end="\t")
client = clickhouse_connect.get_client(host=HOST, port=PORT, username='default', password='')
print("Connected to ClickHouse!\n")

print("This script will clear ALL data from:\n\tdatabase:\t{}\n\tuntil date:\t{}".format(DATABASE, DATE_UNTIL))
print()
print(utils.make_red("This operation cannot be aborted or undone!"))
print(utils.make_red("Are you ABSOLUTELY sure?"))
utils.magic_number_verify()

print(utils.make_red("DOUBLE CHECK THE DATABASE AND DATE!!!"))
utils.magic_number_verify()

print()
print("Getting tables...", end="\t")
tables = utils.get_tables(client, DATABASE)
print("Got {} tables!".format(len(tables)))
print()

for table in tables:
    print("Marking to clear table {:30}".format(table), end="\t")
    # utils.clear_table_by_date(client, DATABASE, table, DATE_UNTIL)
    print("Table marked!")
print()

print("===================================================================")
print("All tables marked to be cleared!")
print("The actual clearing will happen asynchronously in the background.")
print("===================================================================")
