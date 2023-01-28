import datetime
import gzip
import os
import glob
import sys
import time
import random

MAX_BUFFER_LIMIT = 1000000

def make_red(a_string):
    return "\033[91m{}\033[00m".format(a_string)

def magic_number_verify():
    magic_number = random.randint(1e9 + 1, 1e10 - 1)  # 10 digits
    print("Type the magic number '{}' to continue:".format(magic_number))
    answer = input()
    if answer != str(magic_number):
        print("Aborting!")
        exit(1)

def clear_table_by_date(client, a_database, a_table, a_date_until):
    query_string = "ALTER TABLE {}.{} DELETE WHERE date < '{}'".format(a_database, a_table, a_date_until)
    client.query(query_string)


class FakeArgs:
    def __init__(self, database, table, is_snapshot, a_quiet=False):
        self.database = database
        self.table = table
        self.is_snapshot = is_snapshot
        self.quiet = a_quiet


def print_progress(a_total_blocks, a_processed_blocks, process_blocks_start_time):
    seconds_to_finish = int(
        (time.time() - process_blocks_start_time)
        / a_processed_blocks * (a_total_blocks - a_processed_blocks)
    )
    print("Finished a block, progress: {}/{} ({:4.1f}%) ETA: {} (h:m:s)".format(
        a_processed_blocks,
        a_total_blocks,
        a_processed_blocks / a_total_blocks * 100,
        datetime.timedelta(seconds=seconds_to_finish)),
    )


def quiet_print(a_quiet, a_string, **kwargs):
    if not a_quiet:
        print(a_string, **kwargs)
        sys.stdout.flush()


def filter_list(a_list, a_whitelist):
    if a_whitelist == ['*']:
        return a_list
    else:
        return [item for item in a_list if item in a_whitelist]


def purge_folder(args, a_folder_path):
    files = glob.glob(a_folder_path + "/*")
    for f in files:
        quiet_print(args.quiet, "Found file in destination folder, deleting: {}".format(f))
        os.remove(f)


def create_folders(a_folder_path, a_instrument, a_date):
    destination_folder = os.path.join(a_folder_path, str(a_instrument), str(a_date))
    os.makedirs(destination_folder, exist_ok=True)
    return destination_folder


def get_tables(
        client,
        a_database
):
    query_get_tables_string = "SHOW TABLES IN {}".format(a_database)
    query_get_tables_result = client.query(query_get_tables_string)
    tables = []
    for row in query_get_tables_result.result_rows:
        tables.append(row[0])
    tables.sort()
    return tables


def get_instruments(
        client,
        args,
):
    query_get_instruments_string = "SELECT DISTINCT symbol FROM {}.{}".format(args.database, args.table)
    query_get_instruments_result = client.query(query_get_instruments_string)
    instruments = []
    for row in query_get_instruments_result.result_rows:
        instruments.append(row[0])
    instruments.sort()
    return instruments


def get_instrument_dates(
        client,
        args,
        a_instrument
):
    query_get_instrument_dates_string = "SELECT DISTINCT date FROM {}.{} WHERE symbol = '{}'".format(args.database,
                                                                                                     args.table,
                                                                                                     a_instrument)
    query_get_instrument_dates_result = client.query(query_get_instrument_dates_string)
    dates = []
    for row in query_get_instrument_dates_result.result_rows:
        dates.append(str(row[0]))
    dates.sort()
    return dates


def get_batch(
        client,
        args,
        a_symbol,
        a_date,
        a_event_time_min,
        a_event_time_max
):
    query_filters = ["isSnapshot = {}".format(args.is_snapshot),
                     "symbol = '{}'".format(a_symbol),
                     "date = '{}'".format(a_date),
                     "eventTime >= '{}'".format(a_event_time_min),
                     "eventTime <= '{}'".format(a_event_time_max)]

    query_get_batch_string = "SELECT * FROM {}.{} WHERE {} LIMIT {}".format(args.database, args.table,
                                                                            " AND ".join(query_filters),
                                                                            MAX_BUFFER_LIMIT)

    query_start_time = time.time()

    query = client.query(query_get_batch_string)
    # force deflate (for accurate benchmarking) and check if buffer limit reached
    if len(query.result_rows) == MAX_BUFFER_LIMIT:
        quiet_print(args.quiet, "Buffer limit reached!", file=sys.stderr)

    query_end_time = time.time()

    quiet_print(args.quiet, "\tBatch fetch time: {0:0.3f} s".format(query_end_time - query_start_time))
    # quiet_print(args.quiet, "\ta_event_time_min: {}".format(a_event_time_min))
    # quiet_print(args.quiet, "\ta_event_time_max: {}".format(a_event_time_max))
    quiet_print(args.quiet, "\tGot rows: {}".format(int(query.summary['result_rows'])))
    quiet_print(args.quiet, "\tResult size: {0:0.3f} Mb".format(int(query.summary['result_bytes']) // 1024 / 1024))
    # quiet_print(args.quiet, "\tResult summary: {}".format(result.summary))

    return query


def get_min_max_event_time(
        client,
        args,
        a_symbol,
        a_date
):
    query_filters = ["isSnapshot = {}".format(args.is_snapshot),
                     "symbol = '{}'".format(a_symbol),
                     "date = '{}'".format(a_date)]

    query_get_min_event_time_string = "SELECT min(eventTime) FROM {}.{} WHERE {}".format(args.database, args.table,
                                                                                         " AND ".join(query_filters))

    query_get_max_event_time_string = "SELECT max(eventTime) FROM {}.{} WHERE {}".format(args.database, args.table,
                                                                                         " AND ".join(query_filters))

    query_get_min_event_time_result = client.query(query_get_min_event_time_string)
    query_get_max_event_time_result = client.query(query_get_max_event_time_string)

    return [query_get_min_event_time_result.result_rows[0][0], query_get_max_event_time_result.result_rows[0][0]]


def get_count_rows(
        client,
        args,
        a_symbol,
        a_date
):
    query_filters = ["isSnapshot = {}".format(args.is_snapshot),
                     "symbol = '{}'".format(a_symbol),
                     "date = '{}'".format(a_date)]

    query_get_count_row_string = "SELECT count() FROM {}.{} WHERE {}".format(args.database, args.table,
                                                                             " AND ".join(query_filters))

    return client.query(query_get_count_row_string).result_rows[0][0]


class BlockInfo:
    def __init__(self, a_min_event_time, a_max_event_time, a_count_rows):
        self.min_event_time = a_min_event_time
        self.max_event_time = a_max_event_time
        self.count_rows = a_count_rows

    def __str__(self):
        return "\tmin_event_time: {}\n" \
               "\tmax_event_time: {}\n" \
               "\tcount_rows: {}" \
            .format(self.min_event_time,
                    self.max_event_time,
                    self.count_rows)

    def get_intervals(self, intended_batch_size=10000):
        batch_time_interval = int(
            (self.max_event_time - self.min_event_time) / (self.count_rows / intended_batch_size))

        intervals = []

        for i in range(self.min_event_time - 1, self.max_event_time + 1, batch_time_interval):
            intervals.append([i, i + batch_time_interval])
            # quiet_print(args.quiet, "{} - {}".format(intervals[-1][0], intervals[-1][1]))

        # check correctness of intervals:
        assert intervals[0][0] < self.min_event_time
        assert intervals[-1][1] > self.max_event_time

        return intervals


def get_block_info(
        client,
        args,
        a_symbol,
        a_date
):
    min_max_event_time = get_min_max_event_time(client, args, a_symbol, a_date)
    count_rows = get_count_rows(client, args, a_symbol, a_date)

    return BlockInfo(min_max_event_time[0], min_max_event_time[1], count_rows)


class BufferedFileWriter:
    def __init__(self, args, a_file_path):
        self.args = args
        self.file_path = a_file_path
        self.buffer = ""
        if self.args.use_gzip:
            self.file = gzip.open(self.file_path, mode='wb', compresslevel=9)
        else:
            self.file = open(self.file_path, "w")

    def write(self, a_string):
        self.buffer += a_string
        if len(self.buffer) > 1e5:
            self.flush()

    def flush(self):
        if self.args.use_gzip:
            self.file.write(self.buffer.encode('utf-8'))
        else:
            self.file.write(self.buffer)
        self.buffer = ""

    def __del__(self):
        self.flush()
        self.file.close()


class BufferedFileWriterSet:
    def __init__(self, args, a_directory):
        self.directory = a_directory
        if args.use_gzip:
            self.bfw_event_times = BufferedFileWriter(args, os.path.join(self.directory, "event_times.txt.gz"))
            self.bfw_asks_prices = BufferedFileWriter(args, os.path.join(self.directory, "asks_prices.txt.gz"))
            self.bfw_asks_quantities = BufferedFileWriter(args, os.path.join(self.directory, "asks_quantities.txt.gz"))
            self.bfw_bids_prices = BufferedFileWriter(args, os.path.join(self.directory, "bids_prices.txt.gz"))
            self.bfw_bids_quantities = BufferedFileWriter(args, os.path.join(self.directory, "bids_quantities.txt.gz"))
        else:
            self.bfw_event_times = BufferedFileWriter(args, os.path.join(self.directory, "event_times.txt"))
            self.bfw_asks_prices = BufferedFileWriter(args, os.path.join(self.directory, "asks_prices.txt"))
            self.bfw_asks_quantities = BufferedFileWriter(args, os.path.join(self.directory, "asks_quantities.txt"))
            self.bfw_bids_prices = BufferedFileWriter(args, os.path.join(self.directory, "bids_prices.txt"))
            self.bfw_bids_quantities = BufferedFileWriter(args, os.path.join(self.directory, "bids_quantities.txt"))


class Row:
    def __init__(self, a_event_time, a_asks_price, a_asks_quantity, a_bids_price, a_bids_quantity):
        self.event_time = a_event_time
        self.asks_price = a_asks_price
        self.asks_quantity = a_asks_quantity
        self.bids_price = a_bids_price
        self.bids_quantity = a_bids_quantity

    def __lt__(self, other):  # for sorting
        return self.event_time <= other.event_time

    def __str__(self):
        return "\tevent_time: {}\n" \
               "\tasks_price: {}\n" \
               "\tasks_quantity: {}\n" \
               "\tbids_price: {}\n" \
               "\tbids_quantity: {}" \
            .format(self.event_time,
                    self.asks_price,
                    self.asks_quantity,
                    self.bids_price,
                    self.bids_quantity)

    def write_to_files_deduplicated(self, a_prev_event_time, a_buffered_file_writer_set):
        # price_formatter = "{0:0.8f}"
        # quantity_formatter = "{0:0.8f}"
        price_formatter = "{}"
        quantity_formatter = "{}"
        if self.event_time != a_prev_event_time:
            a_buffered_file_writer_set.bfw_event_times.write("{}\n".format(self.event_time))
            a_buffered_file_writer_set.bfw_asks_prices.write(
                "{}\n".format(" ".join(price_formatter.format(ask_price) for ask_price in self.asks_price)))
            a_buffered_file_writer_set.bfw_asks_quantities.write(
                "{}\n".format(" ".join(quantity_formatter.format(ask_quantity) for ask_quantity in self.asks_quantity)))
            a_buffered_file_writer_set.bfw_bids_prices.write(
                "{}\n".format(" ".join(price_formatter.format(bid_price) for bid_price in self.bids_price)))
            a_buffered_file_writer_set.bfw_bids_quantities.write(
                "{}\n".format(" ".join(quantity_formatter.format(bid_quantity) for bid_quantity in self.bids_quantity)))
            return 1
        return 0


def process_block(
        a_client,
        args,
        a_instrument,
        a_instrument_date
):
    quiet_print(args.quiet, "==================================================")
    quiet_print(args.quiet, "Instrument: {0:20} Date: {1:10}".format(a_instrument, a_instrument_date))
    quiet_print(args.quiet, "==================================================\n")
    process_block_start_time = time.time()

    content_folder_path = create_folders(args.root_folder_path, a_instrument, a_instrument_date)
    quiet_print(args.quiet, "Content folder path: {}\n".format(content_folder_path))

    block_info = get_block_info(
        a_client,
        args,
        a_instrument,
        a_instrument_date
    )

    quiet_print(args.quiet, "Block info:\n{}\n".format(block_info))

    intervals = block_info.get_intervals(args.intended_batch_size)

    quiet_print(args.quiet, "Purging folder contents: {}".format(content_folder_path))
    purge_folder(args, content_folder_path)
    quiet_print(args.quiet, "Folder purged!\n")

    buffered_file_writer_set = BufferedFileWriterSet(args, content_folder_path)

    quiet_print(args.quiet, "Dumping {} batches to files:".format(len(intervals)))
    quiet_print(args.quiet, "------------------------------------")

    last_event_time = 0

    for i in range(len(intervals)):
        quiet_print(args.quiet,
                    "Getting batch {} of {} (Instrument: {} Date: {})".format(i + 1, len(intervals), a_instrument,
                                                                              a_instrument_date))
        batch_query = get_batch(
            a_client,
            args,
            a_instrument,
            a_instrument_date,
            intervals[i][0],
            intervals[i][1]
        )
        quiet_print(args.quiet, "Processing batch...")
        processing_start_time = time.time()

        rows = []
        for row in batch_query.result_rows:
            rows.append(Row(row[5], row[8], row[9], row[10], row[11]))
        rows.sort()

        count_row_written = 0

        for row in rows:
            count_row_written += row.write_to_files_deduplicated(last_event_time, buffered_file_writer_set)
            last_event_time = row.event_time

        processing_end_time = time.time()
        quiet_print(args.quiet, "\tBatch processing time: {0:0.3f} s (wrote: {1} rows)".format(
            processing_end_time - processing_start_time,
            count_row_written))
        quiet_print(args.quiet, "------------------------------------")

    process_block_end_time = time.time()
    quiet_print(args.quiet, "==================================================")
    quiet_print(args.quiet,
                "Total block processing time: {0:0.3f} s".format(process_block_end_time - process_block_start_time))
    quiet_print(args.quiet, "==================================================\n")
