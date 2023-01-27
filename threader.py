import random
import subprocess
import time

import utils
import clickhouse_connect
import threading

command_array = ["python3", "clickhouse_dumper.py"]

NUM_THREADS = 20

ROOT_FOLDER_PATH = "./data/"
INTENDED_BATCH_SIZE = 20000
DATABASE = "binance_futures_history"
TABLE = "uDepthUpdates"
IS_SNAPSHOT = 1
HOST = "clickhouse.giant.agtrading.ru"
PORT = 443
INSTRUMENTS_WHITE_LIST = ['*']
DATE_WHITE_LIST = ['*']

command_array.append("--root_folder_path")
command_array.append(ROOT_FOLDER_PATH)

command_array.append("--intended_batch_size")
command_array.append(str(INTENDED_BATCH_SIZE))

command_array.append("--database")
command_array.append(DATABASE)

command_array.append("--table")
command_array.append(TABLE)

command_array.append("--is_snapshot")
command_array.append(str(IS_SNAPSHOT))

command_array.append("--host")
command_array.append(HOST)

command_array.append("--port")
command_array.append(str(PORT))

command_array.append("--quiet")

print("Attempting to connect...", end="\t")
client = clickhouse_connect.get_client(host=HOST, port=PORT, username='default', password='')
print("Connected to ClickHouse!\n")

print("Getting instruments...", end="\t")


class FakeArgs:
    def __init__(self, database, table, is_snapshot, a_quiet=False):
        self.database = database
        self.table = table
        self.is_snapshot = is_snapshot
        self.quiet = a_quiet


args = FakeArgs(DATABASE, TABLE, IS_SNAPSHOT)

instruments = utils.get_instruments(client, args)
instruments = utils.filter_list(instruments, INSTRUMENTS_WHITE_LIST)
print("Got {} instruments after filtering!\n".format(len(instruments)))

blocks = []

for instrument in instruments:
    print("Getting dates for instrument {0:20}".format(instrument + "..."), end="\t")
    instrument_dates = utils.get_instrument_dates(client, args, instrument)
    instrument_dates = utils.filter_list(instrument_dates, DATE_WHITE_LIST)
    print("Got {} dates after filtering!".format(len(instrument_dates)))

    for instrument_date in instrument_dates:
        blocks.append([instrument, instrument_date])


def launch_thread(a_block):
    command_array_copy = command_array.copy()
    command_array_copy.append("--dump_one_block")
    command_array_copy.extend(a_block)
    print("Launching thread for block: {}".format(a_block))
    p = subprocess.Popen(command_array_copy,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)

    for line in iter(p.stdout.readline, b''):
        print(line.decode('utf-8').rstrip())
    print("Thread for block: {} finished!".format(a_block))


blocks.reverse()  # to pop from the 'beginning'

for i in range(NUM_THREADS):
    if len(blocks) == 0:
        break
    block = blocks.pop()
    t = threading.Thread(target=launch_thread, args=(block,))
    t.start()

while True:
    while threading.active_count() > NUM_THREADS:
        time.sleep(1)
    if len(blocks) == 0:
        break
    block = blocks.pop()
    t = threading.Thread(target=launch_thread, args=(block,))
    t.start()

print("All threads launched! Waiting for them to finish...")