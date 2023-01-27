import datetime
import subprocess
import time
import threading
import clickhouse_connect

import utils

command_array = ["python3", "clickhouse_dumper.py"]

NUM_THREADS = 9

ROOT_FOLDER_PATH = "./data/"
INTENDED_BATCH_SIZE = 20000
DATABASE = "binance_futures_history"
TABLE = "uDepthUpdates"
IS_SNAPSHOT = 1
HOST = "clickhouse.giant.agtrading.ru"
PORT = 443
INSTRUMENTS_WHITE_LIST = ['INJ_USDT_PERP']
DATE_WHITE_LIST = ['*']
QUIET = 1
USE_GZIP = 1

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

if QUIET:
    command_array.append("--quiet")

if USE_GZIP:
    command_array.append("--use_gzip")

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
print()

blocks.reverse()  # to pop from the 'beginning'

total_blocks = len(blocks)
processed_blocks = 0


def launch_thread(a_block):
    global processed_blocks

    command_array_copy = command_array.copy()
    command_array_copy.append("--dump_one_block")
    command_array_copy.extend(a_block)
    print("Launching thread for block: {}".format(a_block))
    p = subprocess.Popen(command_array_copy,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)

    for line in iter(p.stdout.readline, b''):
        print(line.decode('utf-8').rstrip())
    processed_blocks += 1


print("==================================================")
print("Starting processing of {} blocks in {} threads:".format(total_blocks, NUM_THREADS))
print("==================================================\n")
process_blocks_start_time = time.time()

for i in range(NUM_THREADS):
    if len(blocks) == 0:
        break
    block = blocks.pop()
    t = threading.Thread(target=launch_thread, args=(block,))
    t.start()

processed_blocks_registered = 0

# Active wait for threads to finish, launch new threads if any blocks left:
while True:
    while processed_blocks_registered == processed_blocks:
        time.sleep(0.01)
    processed_blocks_registered = processed_blocks
    utils.print_progress(total_blocks, processed_blocks, process_blocks_start_time)
    if processed_blocks == total_blocks:
        break
    if len(blocks) > 0:
        block = blocks.pop()
        t = threading.Thread(target=launch_thread, args=(block,))
        t.start()

print("==================================================")
print("All threads finished! Done in {} (h:m:s).".format(
    datetime.timedelta(seconds=int(time.time() - process_blocks_start_time))))
print("==================================================")
