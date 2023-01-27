import time
import clickhouse_connect
import utils

INTENDED_BATCH_SIZE = 100000
DATABASE = "binance_futures_history"
TABLE = "uDepthUpdates"
IS_SNAPSHOT = 1
ROOT_FOLDER_PATH = "./data/"

print("Attempting to connect...", end="\t")
client = clickhouse_connect.get_client(host='clickhouse.giant.agtrading.ru', port=443, username='default', password='')
print("Connected to ClickHouse!\n")

print("Getting instruments...", end="\t")
instruments = utils.get_instruments(client, DATABASE, TABLE)
print("Got {} instruments!\n".format(len(instruments)))

blocks = []

for instrument in instruments:
    print("Getting dates for instrument {0:20}".format(instrument + "..."), end="\t")
    instrument_dates = utils.get_instrument_dates(client, DATABASE, TABLE, instrument)
    for instrument_date in instrument_dates:
        blocks.append([instrument, instrument_date])
    print("Got {} dates!".format(len(instrument_dates)))

for block_id in range(0, len(blocks)):
    instrument = blocks[block_id][0]
    instrument_date = blocks[block_id][1]
    print("==================================================")
    print("Processing block {} of {}".format(block_id + 1, len(blocks)))
    print("Instrument: {0:20} Date: {1:10}".format(instrument, instrument_date))
    print("==================================================\n")
    process_block_start_time = time.time()

    content_folder_path = utils.create_folders(ROOT_FOLDER_PATH, instrument, instrument_date)
    print("Content folder path: {}\n".format(content_folder_path))

    block_info = utils.get_block_info(
        client,
        DATABASE,
        TABLE,
        IS_SNAPSHOT,
        instrument,
        instrument_date
    )

    print("Block info:\n{}\n".format(block_info))

    intervals = block_info.get_intervals(INTENDED_BATCH_SIZE)

    print("Purging folder contents: {}".format(content_folder_path))
    utils.purge_folder(content_folder_path)
    print("Folder purged!\n")

    buffered_file_writer_set = utils.BufferedFileWriterSet(content_folder_path)

    print("Dumping {} batches to files:".format(len(intervals)))
    print("------------------------------------")

    last_event_time = 0

    for i in range(len(intervals)):
        print("Getting batch {} of {}...".format(i + 1, len(intervals)))
        batch_query = utils.get_batch(
            client,
            "binance_futures_history",
            "uDepthUpdates",
            1,
            instrument,
            instrument_date,
            intervals[i][0],
            intervals[i][1]
        )
        print("Processing batch...")
        processing_start_time = time.time()

        rows = []
        for row in batch_query.result_rows:
            rows.append(utils.Row(row[5], row[8], row[9], row[10], row[11]))
        rows.sort()

        count_row_written = 0

        for row in rows:
            count_row_written += row.write_to_files_deduplicated(last_event_time, buffered_file_writer_set)
            last_event_time = row.event_time

        processing_end_time = time.time()
        print("\tBatch processing time: {0:0.3f} s (wrote: {1} rows)".format(
            processing_end_time - processing_start_time,
            count_row_written))
        print("------------------------------------")

    process_block_end_time = time.time()
    print("==================================================")
    print("Total block processing time: {0:0.3f} s".format(process_block_end_time - process_block_start_time))
    print("==================================================\n")
