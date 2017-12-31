import configparser
import os
import sys
import logging
import multiprocessing
from itertools import repeat

from cemaden.src.CemadenRESTfullAPI import CemadenRESTfullAPI
from cemaden.src.CemadenCSV import CemadenCSV


def warp(args):
    CemadenRESTfullAPI(*args)


def warp_csv(args):
    CemadenCSV(*args)


def main():
    # Get the parameters from setup.cfg.
    path_home = os.getcwd() + '/cemaden'
    cfg = configparser.ConfigParser()
    cfg.read(path_home + '/setup.cfg')

    # Create error log file.
    logging.basicConfig(filename=sys.argv[0].split(".")[0] + '.log',
                        format='%(asctime)s\t%(name)s\t[%(process)d]\t'
                               '%(processName)s\t%(threadName)s\t'
                               '%(module)s\t%(funcName)s\t%(lineno)d\t'
                               '%(levelname)s:%(message)s',
                        level=logging.ERROR)

    # List of parameters of configuration to create the connections threads.
    url = []
    host = []
    conn_table = []
    conn_schema = []
    station_type = []
    csv_folder = []

    try:
        for conn in cfg.sections():
            params = cfg.items(conn)
            for param in params:
                if param[0] == 'connection.url':
                    url.append(param[1])
                if param[0] == 'database.host':
                    host.append(param[1])
                if param[0] == 'database.table':
                    conn_table.append(param[1].split(',')[0])
                if param[0] == 'database.schema':
                    conn_schema.append(param[1].split(',')[0])
                if param[0] == 'connection.station_type':
                    station_type.append(param[1])
                if param[0] == 'csv.folder':
                    csv_folder.append(param[1])

        # Rainfall gauge when value is 1 and water level sensor when value is 3.
        # Other value must be invalid (cemaden api documentation).
        if list(set(station_type) - set(['1', '3'])):
            raise Exception('Invalid station type!\nCheck config file parameter %s' % station_type)

        pool = multiprocessing.Pool(len(cfg.sections()))

        client_args = zip(repeat(path_home), cfg.sections(), conn_schema, conn_table, url, station_type)
        pool.map(warp, client_args)

        # Use this code block to import CSV files. Don't forget to comment the 2 prior code lines.
        # client_csv = zip(repeat(path_home), cfg.sections(), conn_schema, conn_table, station_type, csv_folder)
        # pool.map(warp_csv, client_csv)

    except Exception as e:
        logging.error(e)
        exit(0)

if __name__ == '__main__':
    main()
