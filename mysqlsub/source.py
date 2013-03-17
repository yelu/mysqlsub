#!/usr/bin/env python
#coding:utf-8

from connection import Connection
from tools import log
from tools import open_cursor
from tools import get_trace_info
from mysql.connector import utils
from mysql.connector.protocol import MySQLProtocol
from mysql.connector.constants import ServerCmd
from event import *
import json

class Source(object):
    def __init__(self, **kwargs):
        self._socket = None
        self._conf = kwargs
        self._conn = None
        self._tables = {}

    def _query(self, sql):
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
        res, _ = self._query("show master status;")
        print res
        return res[0]
    
    def get_server_id(self):
        return 123456
    
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
        log.debug(str(event))
        if event.is_eof() or event.is_error():
            raise StopIteration
        event_type = EventMap().get_event_type(event.header.event_type)
        if event_type:
            event = event_type(packet)
            log.debug(str(event))
        return event
    
    def add_table(self, db, table, col):
        if db not in self._tables:
            self._tables[db] = {}
        if table not in self._tables[db]:
            self._tables[db][table] = {"columns_info":{}, "do_columns":{}, 
                                       "pos_map":{}}
        for i in col:
            if not isinstance(i, str):
                log.warning("non-string col name.")
                continue
            if i not in self._tables[db][table]["do_columns"]:
                self._tables[db][table]["do_columns"][i] = None
        log.debug(json.dumps(self._tables))
    
    def get_full_columns(self):
        for db, tables in self._tables.items():
            for table, desc in tables.items():
                try:
                    sql = "show full columns from %s.%s" % (db, table)
                    res, _ = self._query(sql)
                    for idx, field in enumerate(res):
                        if field["Field"] in desc["do_columns"]:
                            desc["columns_info"][idx] = \
                            {"name":field["Field"], \
                             "type":field["Type"], \
                             "Default":field["Default"]}
                except:
                    log.warning(get_trace_info())
                    continue
                log.debug(json.dumps(self._tables))
    
    def get_columns_info(self):
        for db, tables in self._tables.items():
            for table, desc in tables.items():
                try:
                    sql = "select * from %s.%s limit 0,0" % (db, table)
                    res, columns_desc = self._query(sql)
                    for idx, field in enumerate(columns_desc):
                        if field[0] in desc["do_columns"]:
                            desc["columns_info"][field[0]] = field
                            desc["pos_map"][idx] = field[0]
                except:
                    log.warning(get_trace_info())
                    continue
                log.debug(json.dumps(self._tables))
            
            
