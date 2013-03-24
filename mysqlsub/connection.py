#!/usr/bin/env python
#coding:utf-8

from mysql.connector import MySQLConnection
from tools import open_cursor


class Connection(MySQLConnection):
    """
    Connection to a MySQL Server, allow access to the underlying socket. 
    """
    def __init__(self, **kwargs):
        super(Connection, self).__init__(**kwargs)
    
    @property
    def socket(self):
        "MySQL connection socket"
        return self._socket
    
    def query(self):
        """
        Query mysql with reconnecting once on failure.
        """
        try:
            res = []
            with open_cursor(self._conn) as cursor:
                cursor.execute(sql)
                columns_desc = cursor.description
                columns = tuple([d[0] for d in columns_desc])
                for row in cursor:
                    res.append(dict(zip(columns, row)))
            return res, columns_desc
        except:
            self.disconnect()
            self.connect()
            res = []
            with open_cursor(self._conn) as cursor:
                cursor.execute(sql)
                columns_desc = cursor.description
                columns = tuple([d[0] for d in columns_desc])
                print cursor.description
                for row in cursor:
                    res.append(dict(zip(columns, row)))
            return res, columns_desc

