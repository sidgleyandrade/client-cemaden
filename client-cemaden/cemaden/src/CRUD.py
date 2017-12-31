import logging
# import psycopg2

from datetime import datetime
from cemaden.src.models import Rainfall
from cemaden.src.models import WaterLevel
from cemaden.src.db.DBConnection import DBConnection


class CRUD:
    def __init__(self, path_home, conn_sec, station_type):
        self.station_type = station_type
        self.set_up(path_home, conn_sec)

    def set_up(self, path_home, conn_sec):
        try:
            self.conn = DBConnection(path_home, conn_sec).connect_database()
            self.cur = self.conn.cursor()
        except Exception as e:
            raise e

    def save(self, data=None, conn_table=None):

        if data:
            list_measure = []
            try:
                for item in data['cemaden']:
                    measure = None
                    if self.station_type == '1':
                        measure = Rainfall()
                    elif self.station_type == '3':
                        measure = WaterLevel()

                    try:
                        measure.name = item['nome']
                        measure.station_id = item['codestacao']
                        measure.city = item['cidade']
                        measure.state = item['uf']
                        if "-" in str(item['dataHora']):
                            measure.datetime = datetime.strptime(item['dataHora'].split('.')[0], "%Y-%m-%d %H:%M:%S")
                        else:
                            measure.datetime = datetime.strptime(item['dataHora'].split('.')[0], "%d/%m/%Y %H:%M:%S")

                        if "-" in str(item['dataHora']):
                            measure.date = datetime.strptime(item['dataHora'].split('.')[0].split(' ')[0], "%Y-%m-%d")
                        else:
                            measure.date = datetime.strptime(item['dataHora'].split('.')[0].split(' ')[0], "%d/%m/%Y")

                        if "," in str(item['longitude']):
                            measure.coordinates = "POINT({} {})".format(str(item['longitude']).replace(',', '.'),
                                                                        str(item['latitude']).replace(',', '.'))
                        else:
                            measure.coordinates = "POINT({} {})".format(item['longitude'], item['latitude'])

                        if str(measure.__class__) == 'cemaden.src.models.Rainfall':
                            if "," in str(item['chuva']):
                                measure.measure = item['chuva'].replace(',', '.')
                            else:
                                measure.measure = item['chuva']

                        if str(measure.__class__) == 'cemaden.src.models.WaterLevel':
                            if "," in str(item['nivel']):
                                measure.measure = item['nivel'].replace(',', '.')
                            else:
                                measure.measure = item['nivel']
                            if "," in str(item['offset']):
                                measure.offset_level = item['offset'].replace(',', '.')
                            else:
                                measure.offset_level = item['offset']

                    except Exception as e:
                        logging.error(item)
                        logging.error(e)

                    if measure.measure == None:
                        continue

                    list_measure.append(measure)

                self.cur.execute("""SET TimeZone = 'UTC' """)
                insert = []

                for measure in list_measure:
                    if str(measure.__class__) == 'cemaden.src.models.Rainfall':
                        insert.append({"station_id":measure.station_id,
                                       "name": measure.name,
                                       "datetime": measure.datetime,
                                       "date": measure.date,
                                       "state": measure.state,
                                       "city": measure.city,
                                       "measure": measure.measure,
                                       "measure_cur": measure.measure,
                                       "coordinates": measure.coordinates})

                    if str(measure.__class__) == 'cemaden.src.models.WaterLevel':
                        insert.append({"station_id": measure.station_id,
                                       "name": measure.name,
                                       "datetime": measure.datetime,
                                       "date": measure.date,
                                       "state": measure.state,
                                       "city": measure.city,
                                       "measure": measure.measure,
                                       "measure_cur": measure.measure,
                                       "offset_level": measure.offset_level,
                                       "offset_level_cur": measure.offset_level,
                                       "coordinates": measure.coordinates})

                if str(measure.__class__) == 'cemaden.src.models.Rainfall':
                    self.cur.executemany("""INSERT INTO """ + conn_table + """
                                        (station_id, name, datetime, date,
                                        state, city, measure, coordinates)
                                        VALUES (%(station_id)s, %(name)s, %(datetime)s,
                                                %(date)s, %(state)s, %(city)s, %(measure)s,
                                                ST_GeomFromText(%(coordinates)s, 4326))
                                        ON CONFLICT (station_id, coordinates, datetime) DO
                                        UPDATE SET measure = %(measure_cur)s""", insert)

                if str(measure.__class__) == 'cemaden.src.models.WaterLevel':
                    self.cur.executemany("""INSERT INTO """ + conn_table + """ 
                                        (station_id, name, datetime, date, state, 
                                        city, measure, offset_level, coordinates)
                                        VALUES (%(station_id)s, %(name)s, %(datetime)s, 
                                                %(date)s, %(state)s, %(city)s, %(measure)s, 
                                                %(offset_level)s, ST_GeomFromText(%(coordinates)s, 4326))
                                        ON CONFLICT (station_id, coordinates, datetime) DO
                                        UPDATE SET  measure = %(measure_cur)s, 
                                                    offset_level = %(offset_level_cur)s""", insert)
                self.conn.commit()
            # except psycopg2.IntegrityError:
            #    self.conn.rollback()
            except Exception as e:
                self.conn.rollback()
                logging.error(e)
                raise e
