import os
import glob
import sys
import time

MAX_BUFFER_LIMIT = 1000000


def purge_folder(a_folder_path):
    files = glob.glob(a_folder_path + "/*")
    for f in files:
        print("Found file in destination folder, deleting: {}".format(f))
        os.remove(f)


def create_folders(a_folder_path, a_instrument, a_date):
    destination_folder = os.path.join(a_folder_path, str(a_instrument), str(a_date))
    os.makedirs(destination_folder, exist_ok=True)
    return destination_folder


def get_instruments(
        client,
        a_database,
        a_table
):
    query_get_instruments_string = "SELECT DISTINCT symbol FROM {}.{}".format(a_database, a_table)
    query_get_instruments_result = client.query(query_get_instruments_string)
    instruments = []
    for row in query_get_instruments_result.result_rows:
        instruments.append(row[0])
    instruments.sort()
    return instruments


def get_instrument_dates(
        client,
        a_database,
        a_table,
        a_instrument
):
    query_get_instrument_dates_string = "SELECT DISTINCT date FROM {}.{} WHERE symbol = '{}'".format(a_database,
                                                                                                     a_table,
                                                                                                     a_instrument)
    query_get_instrument_dates_result = client.query(query_get_instrument_dates_string)
    dates = []
    for row in query_get_instrument_dates_result.result_rows:
        dates.append(str(row[0]))
    dates.sort()
    return dates


def get_batch(
        client,
        a_database,
        a_table,
        a_is_snapshot,
        a_symbol,
        a_date,
        a_event_time_min,
        a_event_time_max
):
    query_filters = ["isSnapshot = {}".format(a_is_snapshot),
                     "symbol = '{}'".format(a_symbol),
                     "date = '{}'".format(a_date),
                     "eventTime >= '{}'".format(a_event_time_min),
                     "eventTime <= '{}'".format(a_event_time_max)]

    query_get_batch_string = "SELECT * FROM {}.{} WHERE {} LIMIT {}".format(a_database, a_table,
                                                                            " AND ".join(query_filters),
                                                                            MAX_BUFFER_LIMIT)

    query_start_time = time.time()

    query = client.query(query_get_batch_string)
    # force deflate (for accurate benchmarking) and check if buffer limit reached
    if len(query.result_rows) == MAX_BUFFER_LIMIT:
        print("Buffer limit reached!", file=sys.stderr)

    query_end_time = time.time()

    print("\tBatch fetch time: {0:0.3f} s".format(query_end_time - query_start_time))
    print("\ta_event_time_min: {}".format(a_event_time_min))
    print("\ta_event_time_max: {}".format(a_event_time_max))
    print("\tGot rows: {}".format(int(query.summary['result_rows'])))
    print("\tResult size: {0:0.3f} Mb".format(int(query.summary['result_bytes']) // 1024 / 1024))
    # print("\tResult summary: {}".format(result.summary))

    return query


def get_min_max_event_time(
        client,
        a_database,
        a_table,
        a_is_snapshot,
        a_symbol,
        a_date
):
    query_filters = ["isSnapshot = {}".format(a_is_snapshot),
                     "symbol = '{}'".format(a_symbol),
                     "date = '{}'".format(a_date)]

    query_get_min_event_time_string = "SELECT min(eventTime) FROM {}.{} WHERE {}".format(a_database, a_table,
                                                                                         " AND ".join(query_filters))

    query_get_max_event_time_string = "SELECT max(eventTime) FROM {}.{} WHERE {}".format(a_database, a_table,
                                                                                         " AND ".join(query_filters))

    query_get_min_event_time_result = client.query(query_get_min_event_time_string)
    query_get_max_event_time_result = client.query(query_get_max_event_time_string)

    return [query_get_min_event_time_result.result_rows[0][0], query_get_max_event_time_result.result_rows[0][0]]


def get_count_rows(
        client,
        a_database,
        a_table,
        a_is_snapshot,
        a_symbol,
        a_date
):
    query_filters = ["isSnapshot = {}".format(a_is_snapshot),
                     "symbol = '{}'".format(a_symbol),
                     "date = '{}'".format(a_date)]

    query_get_count_row_string = "SELECT count() FROM {}.{} WHERE {}".format(a_database, a_table,
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
            # print("{} - {}".format(intervals[-1][0], intervals[-1][1]))

        # check correctness of intervals:
        assert intervals[0][0] < self.min_event_time
        assert intervals[-1][1] > self.max_event_time

        return intervals


def get_block_info(
        client,
        a_database,
        a_table,
        a_is_snapshot,
        a_symbol,
        a_date
):
    min_max_event_time = get_min_max_event_time(client, a_database, a_table, a_is_snapshot, a_symbol, a_date)
    count_rows = get_count_rows(client, a_database, a_table, a_is_snapshot, a_symbol, a_date)

    return BlockInfo(min_max_event_time[0], min_max_event_time[1], count_rows)


class BufferedFileWriter:
    def __init__(self, a_file_name):
        self.file_name = a_file_name
        self.buffer = ""
        self.file = open(self.file_name, "w")

    def write(self, a_string):
        self.buffer += a_string
        if len(self.buffer) > 1e5:
            self.flush()

    def flush(self):
        # print("FLUSHED", file=sys.stderr)
        self.file.write(self.buffer)
        self.buffer = ""

    def __del__(self):
        self.flush()
        self.file.close()


class BufferedFileWriterSet:
    def __init__(self, a_directory):
        self.directory = a_directory
        self.bfw_event_times = BufferedFileWriter(os.path.join(self.directory, "event_times.txt"))
        self.bfw_asks_prices = BufferedFileWriter(os.path.join(self.directory, "asks_prices.txt"))
        self.bfw_asks_quantities = BufferedFileWriter(os.path.join(self.directory, "asks_quantities.txt"))
        self.bfw_bids_prices = BufferedFileWriter(os.path.join(self.directory, "bids_prices.txt"))
        self.bfw_bids_quantities = BufferedFileWriter(os.path.join(self.directory, "bids_quantities.txt"))


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
