import psycopg2
import configparser


class DBConnection(object):

    def __init__(self, path_home='', conn_sec=''):
        try:
            self.path_home = path_home
            self.conn = None
            self.conn_sec = conn_sec
            self.set_database_params()
        except Exception as e:
            raise e

    def set_database_params(self):
        cfg = configparser.ConfigParser()
        cfg.read(self.path_home + '/setup.cfg')

        params = cfg.items(self.conn_sec)

        for param in params:
            if param[0] == 'database.host':
                self.host = param[1].split(',')[0]
            if param[0] == 'database.schema':
                self.schema = param[1].split(',')[0]
            if param[0] == 'database.name':
                self.dbname = param[1].split(',')[0]
            if param[0] == 'database.user':
                self.user = param[1].split(',')[0]
            if param[0] == 'database.password':
                self.password = param[1].split(',')[0]

        return False if not all([self.host, self.dbname,
                                 self.user, self.password]) else True

    def connect_database(self):
        if self.conn is None:
            conn_str = "host='%s' dbname='%s' user='%s' password='%s'" % \
                       (self.host, self.dbname, self.user, self.password)
            try:
                self.conn = psycopg2.connect(conn_str)
            except Exception as e:
                raise e

        return self.conn
