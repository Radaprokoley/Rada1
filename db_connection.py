import pymysql
import simplejson as json


class zion_db_connection:

    def __init__(self):
        self.conn = None

    def conn_open(self):
        with open('Data_values.json') as f:
            data_value_list = json.load(f)
            username = data_value_list[0]['db_username']
            password = data_value_list[0]['db_password']
            conn = pymysql.connect(host='127.0.0.1', user=username, passwd=password, db='SampleTrackerDB',
                                   connect_timeout=20, port=3340)
        return conn

    def conn_close(self, conn):
        conn.close()
