#!/usr/bin/env python
#coding:utf-8

from mysql.connector import MySQLConnection


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

