__title__ = 'Virtual Hydrological Gauge'
__author__ = 'Sidgley Camargo de Andrade'
__license__ = 'GPLv3'

import configparser as ConfigParser
import logging
import time
from datetime import datetime, timedelta
from email.utils import parsedate_tz
from threading import Thread

from pymongo import *

try:
    from TwitterBean import *
    from TwitterDAO import TwitterDAO
except ImportError as e:
    raise e


class CrawlerDBImporter(Thread):
    ''' Access REST API resources.
    
    :param cfg_access: ....
    :param station_type: "1" rainfall gauge (default) or "3" hydrological
    '''

    def __init__(self, path_home=''):
        Thread.__init__(self)
        try:
            self.path_home = path_home
            self._set_api_params()
            self.run()
        except Exception as e:
            raise e

    def run(self):
        try:
            while True:
                self._parser_tweet()
                time.sleep(self.timer)
                self._set_api_params()
        except Exception as e:
            logging.error(e)
            pass

    def _set_api_params(self, ):
        ''' initialize mongodb connection parameters '''
        cfg = ConfigParser.ConfigParser()

        cfg.read(self.path_home + 'src/setup.cfg')
        params = cfg.items('crawler_tweets')

        for param in params:
            if param[0] == 'crawler.host':
                self.host = param[1]
            elif param[0] == 'crawler.port':
                self.port = param[1]
            elif param[0] == 'crawler.dbname':
                self.dbname = param[1]
            elif param[0] == 'crawler.collection':
                self.collection = param[1]
            elif param[0] == 'crawler.timer':
                self.timer = int(param[1])

        return False if not all([self.host, self.dbname, self.collection]) else True

    def _parser_tweet(self):

        try:
            client = MongoClient()
            client = MongoClient("mongodb://" + self.host + ":" + self.port)
            db = client[self.dbname]
            coll = db[self.collection]

            cursor = coll.find()

            ''' tweets '''
            obj = TwitterBean()

            for document in cursor:

                tweets = list()

                try:

                    try:
                        obj.idt = document['id']
                    except Exception as e:
                        raise e

                    try:
                        obj.id_str = document['id_str']
                    except Exception as e:
                        raise e

                    try:
                        obj.created_at = str(self.to_datetime(document['created_at']))
                        obj.date = obj.created_at[:-9]
                        obj.time = obj.created_at[10:]
                    except Exception as e:
                        raise e

                    try:
                        obj.text = document['text']
                    except Exception as e:
                        raise e

                    try:
                        obj.retweeted = document['retweeted']
                    except Exception as e:
                        raise e

                    try:
                        hashtag_list = list()
                        l = document['entities']['hashtags']
                        for item in l:
                            hashtag_list.append(item['text'])
                        obj.hashtags = hashtag_list
                    except Exception as e:
                        pass

                    try:
                        obj.user_id_str = document['user']['id_str']
                    except Exception as e:
                        raise e

                    try:
                        obj.user_name = document['user']['name']
                    except Exception as e:
                        raise e

                    try:
                        obj.user_screen_name = document['user']['screen_name']
                    except Exception as e:
                        raise e

                    try:
                        obj.place_type = document['place']['place_type']
                    except Exception as e:
                        pass

                    try:
                        obj.place_name = document['place']['name']
                    except Exception as e:
                        pass

                    try:
                        obj.place_name = document['place']['name']
                    except Exception as e:
                        pass

                    try:
                        obj.place_fullname = document['place']['full_name']
                    except Exception as e:
                        pass

                    full_name = document['place']['full_name'].split(", ")

                    try:
                        obj.place_state = full_name[1]
                    except Exception as e:
                        pass

                    try:
                        obj.place_country = document['place']['country']
                    except Exception as e:
                        pass

                    try:
                        obj.coordinates = "POINT({} {})".format(document['coordinates']['coordinates'][0],
                                                                document['coordinates']['coordinates'][1])

                    except Exception as e:
                        pass

                    # print(obj._print())

                    tweets.append(obj)

                    TwitterDAO(self.path_home).save(tweets)

                except Exception as e:
                    logging.error(e)
                    logging.error(document)
                    pass

        except Exception as e:
            logging.error(e)
            pass

    def to_datetime(self, datestring):
        time_tuple = parsedate_tz(datestring.strip())
        dt = datetime(*time_tuple[:6])
        return dt - timedelta(seconds=time_tuple[-1])

# if __name__ == '__main__':
#    crawler = CrawlerDBImporter('/home/sidgleyandrade/Documents/Dropbox/MyAppProjects/workspace-python/virtualHydrologicalGauge/virtualHydrologicalGauge/')
