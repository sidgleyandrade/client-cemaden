import logging
import time
import csv
import shutil
import os
import json
import ast

from cemaden.src.CRUD import CRUD
from cemaden.src.db.DBConnection import DBConnection


class CemadenCSV:

    def __init__(self, path_home, conn_sec, conn_schema, conn_table, station_type, csv_folder, timer=300):
        """ Define the initial parameters and create the client api 
        for fetching and storing the measurements.
        """
        self.path_home = path_home
        self.conn_sec = conn_sec
        self.conn_schema = conn_schema
        self.conn_table = conn_table
        self.station_type = station_type
        self.csv_folder = csv_folder
        self.running = False
        self.timer = timer

        # Create database connection to store the data
        self.CRUD = CRUD(self.path_home, self.conn_sec, self.station_type)

        # Create database table if it does not exist
        self.create_table()

        while True:
            if not self.running:
                self.init()

    def create_table(self):
        """ Create the sensors's database table from the template
        (file template-table.sql).
        """
        try:
            conn = DBConnection(self.path_home, self.conn_sec).connect_database()

            try:
                template = open(self.path_home + '/template-table-type-' +
                                self.station_type + '.sql', 'r').read() % \
                           (str(self.conn_schema), str(self.conn_table),
                            str(self.conn_schema), str(self.conn_table),
                            str(self.conn_schema), str(self.conn_table),
                            str(self.conn_schema), str(self.conn_table),
                            str(self.conn_table), str(self.conn_table),
                            str(self.conn_schema), str(self.conn_table))
                cur = conn.cursor()
                cur.execute(template)
                conn.commit()
            except Exception as e:
                raise e
            finally:
                conn.close()

        except Exception as e:
            logging.error(e)
            pass

    def init(self):
        try:
            self.running = True
            while True:
                files = [s for s in os.listdir(
                    os.path.expanduser(self.path_home + '/' + self.csv_folder + '/')) if s.endswith('.csv')]

                files.sort()

                for file in files:
                    try:
                        print('Processing file %s' % file)
                        with open(self.path_home + '/' + self.csv_folder + '/' + file) as f:
                            reader = csv.DictReader(f, delimiter=';', quoting=csv.QUOTE_NONE)

                            header = reader.fieldnames
                            header = ['codestacao' if x in ['codEstacao'] else x for x in header]
                            header = ['codestacao' if x in ['COD_ESTACAO'] else x for x in header]

                            header = ['nome' if x in ['nomeEstacao'] else x for x in header]
                            header = ['nome' if x in ['NOME_ESTACAO'] else x for x in header]

                            header = ['cidade' if x in ['municipio'] else x for x in header]
                            header = ['cidade' if x in ['MUNICIPIO'] else x for x in header]

                            header = ['uf' if x in ['UF'] else x for x in header]

                            header = ['longitude' if x in ['LONGITUDE'] else x for x in header]
                            header = ['latitude' if x in ['LATITUDE'] else x for x in header]

                            header = ['dataHora' if x in ['DATA_HORA'] else x for x in header]
                            header = ['dataHora' if x in ['datahora'] else x for x in header]

                            header = ['chuva' if x in ['valorMedida'] else x for x in header]
                            header = ['chuva' if x in ['MEDIDA'] else x for x in header]

                            reader.fieldnames = header
                            del header

                            c = 0
                            for row in reader:
                                data_list = list()
                                data_list.append(row)

                                data_list = "{'cemaden':" + str(data_list) + "}"
                                raw_data = json.dumps(data_list)
                                raw_data = json.loads(raw_data)
                                raw_data = ast.literal_eval(raw_data)

                                c += 1
                                print(c)
                                self.CRUD.save(data=raw_data, conn_table=self.conn_schema + '.' + self.conn_table)

                                del raw_data
                                del data_list

                            del reader

                        shutil.move(self.path_home + '/' + self.csv_folder + '/' + file,
                                    self.path_home + '/' + self.csv_folder + '/' + file + '.processed')
                        print('File %s processed!' % file)
                    except Exception as e:
                        logging.error(e)
                        shutil.move(self.path_home + '/' + self.csv_folder + '/' + file,
                                    self.path_home + '/' + self.csv_folder + '/' + file + '.error')
                        print('ERROR: file %s' % file)
                        break

                time.sleep(self.timer)

        except Exception as e:
            self.running = False
            logging.error(e)
            pass
