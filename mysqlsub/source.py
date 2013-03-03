#!/usr/bin/env python
#coding:utf-8

from connection import Connection
from tools import log
from tools import open_cursor
from mysql.connector import utils
from mysql.connector.protocol import MySQLProtocol
from mysql.connector.constants import ServerCmd
from event import BinlogEvent

class Source(object):
    def __init__(self, **kwargs):
        self._socket = None
        self._conf = kwargs
        self._conn = None

    def _query(self, sql):
        """
        Query mysql with reconnecting once on failure.
        """
        try:
            res = []
            with open_cursor(self._conn) as cursor:
                cursor.execute(sql)
                columns = tuple([d[0] for d in cursor.description])
                for row in cursor:
                    res.append(dict(zip(columns, row)))
            return res
        except:
            self.disconnect()
            self.connect()
            res = []
            with open_cursor(self._conn) as cursor:
                cursor.execute(sql)
                columns = tuple([d[0] for d in cursor.description])
                for row in cursor:
                    res.append(dict(zip(columns, row)))
            return res
        
    def connect(self):
        """
        build connection to mysql server.
        """
        self._conn = Connection(**(self._conf))
        self._conn.connect()
        self._socket = self._conn.socket
    
    def disconnect(self):
        """
        disconnect mysql server.
        """
        self._conn.close()
        self._socket = None

    def show_master_status(self):
        res = self._query("show master status;")
        print res
        return res[0]
    
    def get_server_id(self):
        return 1
    
    def binlog_dump(self, log_file, offset):
        """
        COM_BINLOG_DUMP
        +=============================================+
        | packet header    |  packet length    0 : 3 |   
        |                  +-------------------------+   
        |                  |  sequence number  3 : 1 |
        +============================================+
        | command packet   |  command code     4 : 1 |    COM_BINLOG_DUMP
        |                  +------------------------―+
        |                  |  offset           5 : 4 |
        |                  +-------------------------+
        |                  |  flags            9 : 2 |
        |                  +-------------------------+
        |                  |  server id        11 : 4|
        |                  +-------------------------+
        |                  |  log name         15 : x|
        +============================================+
        """
        
        payload = ''
        payload += utils.int1store(ServerCmd.BINLOG_DUMP)
        payload += utils.int4store(offset)
        payload += utils.int2store(0)
        payload += utils.int4store(self.get_server_id()) 
        payload += log_file
        payload += '\x00'
        log.debug("len(payload) = %d" % len(payload))
        
        # send BIGLOGDUMP command and parse ok packet response.
        self._socket.send(payload, 0)
        ok_packet = self._socket.recv()
        parser = MySQLProtocol()
        ok_packet = parser.parse_ok(ok_packet)
        print ok_packet
    
    def __iter__(self):
        return self  
        
    def next(self):
        packet = self._socket.recv()
        event = BinlogEvent(packet)
        if event.is_eof() or event.is_error():
            raise StopIteration
        event.header.debug()
        return event
            
