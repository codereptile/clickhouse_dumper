import datetime
import subprocess
import time
import threading
import clickhouse_connect

import utils

##############################################
# CONFIG
JUST_PRINT = 0

NUM_THREADS = 30
INTENDED_BATCH_SIZE = 20000

ROOT_FOLDER_PATH = "./binance_futures_all_instruments_2022-11-12_2023-01-08"

DATABASE = "restore_binance_futures_history_backup_all_tables_08_01_2023"
TABLE = "uDepthUpdates"
IS_SNAPSHOT = 1
INSTRUMENTS_WHITE_LIST = ['.*']
DATE_WHITE_LIST = ['.*']

HOST = "clickhouse.giant.agtrading.ru"
PORT = 443

QUIET = 1
USE_GZIP = 1
##############################################

utils.quiet_print(False, "Attempting to connect...", end="\t")
client = clickhouse_connect.get_client(host=HOST, port=PORT, username='default', password='')
utils.quiet_print(False, "Connected to ClickHouse!\n")

utils.quiet_print(False, "Getting instruments...", end="\t")
instruments = utils.get_instruments(client, DATABASE, TABLE)
instruments = utils.filter_list_whitelist(instruments, INSTRUMENTS_WHITE_LIST)
instruments.sort()
utils.quiet_print(False, "Got {} instruments after filtering!\n".format(len(instruments)))

instrument_dates = {}

for instrument in instruments:
    utils.quiet_print(False, "Getting dates for instrument {0:20}".format(instrument + "..."), end="\t")
    instrument_dates[instrument] = utils.get_instrument_dates(client, DATABASE, TABLE, instrument)
    instrument_dates[instrument] = utils.filter_list_whitelist(instrument_dates[instrument], DATE_WHITE_LIST)
    instrument_dates[instrument].sort()
    utils.quiet_print(False, "Got {} dates after filtering!".format(len(instrument_dates[instrument])))
utils.quiet_print(False, )

utils.quiet_print(False,
                  "====================================================================================================")
utils.quiet_print(False, "{:^100}".format("Selected instruments and dates:"))
utils.print_instrument_dates_table(instrument_dates)
utils.quiet_print(False,
                  "====================================================================================================")

if JUST_PRINT:
    exit()

blocks = []

for instrument in instruments:
    for date in instrument_dates[instrument]:
        blocks.append([instrument, date])

blocks.reverse()  # to pop from the 'beginning'

total_blocks = len(blocks)
processed_blocks = 0
count_errors = 0


def launch_thread(a_block):
    global processed_blocks, count_errors

    command_array_copy = command_array.copy()
    command_array_copy.append("--dump_one_block")
    command_array_copy.extend(a_block)
    utils.quiet_print(False, "Launching thread for block: {}".format(a_block))
    p = subprocess.Popen(command_array_copy,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)

    for line in iter(p.stdout.readline, b''):
        utils.quiet_print(False, line.decode('utf-8').rstrip())
    while p.poll() is None:
        time.sleep(0.1)
    if p.returncode != 0:
        utils.quiet_print(False, utils.make_red(
            "Error while processing block: Instrument: {}, Date: {}, return code: {}".format(a_block[0], a_block[1],
                                                                                             p.returncode)))
        count_errors += 1
    processed_blocks += 1


utils.quiet_print(False, "{:^100}".format(
    "Starting processing of {} blocks in up to {} threads:".format(total_blocks, NUM_THREADS)))
utils.quiet_print(False,
                  "====================================================================================================\n")

##############################################
# Create command array:
command_array = [
    "python3", "clickhouse_dumper.py",
    "--root_folder_path", ROOT_FOLDER_PATH,
    "--intended_batch_size", str(INTENDED_BATCH_SIZE),
    "--database", DATABASE,
    "--table", TABLE,
    "--is_snapshot", str(IS_SNAPSHOT),
    "--host", HOST,
    "--port", str(PORT)
]
if QUIET:
    command_array.append("--quiet")

if USE_GZIP:
    command_array.append("--use_gzip")
##############################################

process_blocks_start_time = time.time()

for i in range(NUM_THREADS):
    if len(blocks) == 0:
        break
    block = blocks.pop()
    t = threading.Thread(target=launch_thread, args=(block,))
    t.start()
    time.sleep(0.1)

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

utils.quiet_print(False,
                  "====================================================================================================")
if count_errors > 0:
    utils.quiet_print(False, utils.make_red("Finished with {} errors!".format(count_errors)))
utils.quiet_print(False, "All threads finished! Done in {} (h:m:s).".format(
    datetime.timedelta(seconds=int(time.time() - process_blocks_start_time))))
utils.quiet_print(False,
                  "====================================================================================================\n")
