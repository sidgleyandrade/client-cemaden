import logging
import time
import requests

from cemaden.src.CRUD import CRUD
from cemaden.src.db.DBConnection import DBConnection


class CemadenRESTfullAPI:

    def __init__(self, path_home, conn_sec, conn_schema, conn_table,
                 url, station_type, timer=600):
        """ Define the initial parameters and create the client api 
        for fetching and storing the measurements.
        """
        self.path_home = path_home
        self.conn_sec = conn_sec
        self.conn_schema = conn_schema
        self.conn_table = conn_table
        self.url = url
        self.station_type = station_type
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
            MyStreamListener(crud=self.CRUD,
                             conn_sec=self.conn_sec,
                             conn_schema=self.conn_schema,
                             conn_table=self.conn_table,
                             url=self.url,
                             station_type=self.station_type,
                             timer=self.timer)
        except Exception as e:
            self.running = False
            logging.error(e)
            pass


class MyStreamListener:
    def __init__(self, crud, conn_sec, conn_schema, conn_table, url,
                 station_type, timer):
        self.crud = crud
        self.conn_sec = conn_sec
        self.conn_schema = conn_schema
        self.conn_table = conn_table
        self.url = url
        self.station_type = station_type
        self.timer = timer
        self.on_data()

    def on_data(self):
        try:
            while True:
                raw_data = self.request()
                self.crud.save(data=raw_data, conn_table=self.conn_schema + '.' + self.conn_table)
                time.sleep(self.timer)
        except Exception as e:
            raise e

    def request(self):
        raw_data = None
        try:
            response = requests.get(self.url + self.station_type)
        except Exception as e:
            raise e

        if response.status_code == 200 and len(response.content) > 14:
            raw_data = response.json()
        response.raw.close()

        return raw_data


class CemadenResponse(object):

    def __init__(self, response):
        self.response = response

    @property
    def headers(self):
        """:returns: Dictionary of API response header contents."""
        return self.response.headers

    @property
    def status_code(self):
        """:returns: HTTP response status code."""
        return self.response.status_code

    @property
    def text(self):
        """:returns: Raw API response text."""
        return self.response.text

    def json(self):
        """:returns: response as JSON object."""
        return self.response.json()
