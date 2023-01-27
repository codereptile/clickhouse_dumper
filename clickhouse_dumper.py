import clickhouse_connect
import utils
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("--root_folder_path", dest="root_folder_path", default=['./data'], nargs='?')
parser.add_argument("--intended_batch_size", dest="intended_batch_size", default=100000, nargs='?', type=int)
parser.add_argument("--database", dest="database", default="binance_futures_history", nargs='?')
parser.add_argument("--table", dest="table", default="uDepthUpdates", nargs='?')
parser.add_argument("--is_snapshot", dest="is_snapshot", default=1, nargs='?')
parser.add_argument("--host", dest="host", default="clickhouse.giant.agtrading.ru", nargs='?')
parser.add_argument("--port", dest="port", default=443, nargs='?', type=int)
parser.add_argument("--dump_one_block", dest="dump_one_block", default=None, nargs='+')
parser.add_argument("--instrument_white_list", dest="instrument_white_list", default=['*'], nargs='+')
parser.add_argument("--date_white_list", dest="date_white_list", default=['*'], nargs='+')
parser.add_argument("--quiet", dest="quiet", const=1, default=0, nargs='?')
args = parser.parse_args()

utils.quiet_print(args.quiet,
                  "####################################################################################################")
utils.quiet_print(args.quiet, "Launching clickhouse dumper with the following arguments:")

utils.quiet_print(args.quiet, "ROOT_FOLDER_PATH: {}".format(args.root_folder_path))
utils.quiet_print(args.quiet, "INTENDED_BATCH_SIZE: {}".format(args.intended_batch_size))
utils.quiet_print(args.quiet, "DATABASE: {}".format(args.database))
utils.quiet_print(args.quiet, "TABLE: {}".format(args.table))
utils.quiet_print(args.quiet, "IS_SNAPSHOT: {}".format(args.is_snapshot))
utils.quiet_print(args.quiet, "HOST: {}".format(args.host))
utils.quiet_print(args.quiet, "PORT: {}".format(args.port))
utils.quiet_print(args.quiet, "DUMP_ONE_BLOCK: {}".format(args.dump_one_block))
utils.quiet_print(args.quiet, "INSTRUMENT_WHITE_LIST: {}".format(args.instrument_white_list))
utils.quiet_print(args.quiet, "DATE_WHITE_LIST: {}".format(args.date_white_list))
utils.quiet_print(args.quiet, "QUIET: {}".format(args.quiet))

utils.quiet_print(args.quiet, "Attempting to connect...", end="\t")
client = clickhouse_connect.get_client(host=args.host, port=args.port, username='default', password='')
utils.quiet_print(args.quiet, "Connected to ClickHouse!\n")

blocks = []
if args.dump_one_block is None:
    utils.quiet_print(args.quiet, "Getting instruments...", end="\t")
    instruments = utils.get_instruments(client, args.database, args.table)
    # utils.quiet_print(args.quiet, "Got {} instruments!".format(len(instruments)), end="\t")
    instruments = utils.filter_list(instruments, args.instrument_white_list)
    utils.quiet_print(args.quiet, "Got {} instruments after filtering!\n".format(len(instruments)))

    for instrument in instruments:
        utils.quiet_print(args.quiet, "Getting dates for instrument {0:20}".format(instrument + "..."), end="\t")
        instrument_dates = utils.get_instrument_dates(client, args.database, args.table, instrument)
        # utils.quiet_print(args.quiet, "Got {} dates!".format(len(instrument_dates)), end="\t")
        instrument_dates = utils.filter_list(instrument_dates, args.date_white_list)
        utils.quiet_print(args.quiet, "Got {} dates after filtering!".format(len(instrument_dates)))

        for instrument_date in instrument_dates:
            blocks.append([instrument, instrument_date])
else:
    assert len(args.dump_one_block) == 2, "dump_one_block should be in format instrument,date"
    utils.quiet_print(args.quiet,
                      "Dumping one block: instrument: {}, date: {}".format(args.dump_one_block[0], args.dump_one_block[1]))
    blocks.append([args.dump_one_block[0], args.dump_one_block[1]])

for block_id in range(0, len(blocks)):
    instrument = blocks[block_id][0]
    instrument_date = blocks[block_id][1]
    utils.quiet_print(args.quiet, "Processing block {} of {}".format(block_id + 1, len(blocks)))
    utils.process_block(client, args, instrument, instrument_date)

utils.quiet_print(args.quiet,
                  "####################################################################################################")
