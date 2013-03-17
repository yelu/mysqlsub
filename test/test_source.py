'''
Created on 2013-2-27

@author: yelu01
'''

import os
import time
os.sys.path.append("/home/yelu/mycode/mysqlsub/")
import unittest
from mysqlsub import source
from mysqlsub.constants import EventType
from mysqlsub.event import *

'''
class TestSource(unittest.TestCase):
    def setUp(self):
        self._source = source.Source(host = "10.48.78.23",
                        port = 5858,
                        user = "yelu",
                        password = "yelu123456")
        pass

    def tearDown(self):
        self._source.disconnect()
        pass

    def testName(self):
        self._source.connect()
        self._source.show_master_status()
        pass


class TestBinlogdump(unittest.TestCase):

    def setUp(self):
        self._source = source.Source(host = "10.48.78.23",
                        port = 5858,
                        user = "yelu",
                        password = "yelu123456")

    def tearDown(self):
        self._source.disconnect()

    def testName(self):
        self._source.connect()
        self._source.binlog_dump("mysql-bin.000299", 386074393)

      

class TestIter(unittest.TestCase):

    def setUp(self):
        self._source = source.Source(host = "10.48.78.23",
                        port = 5858,
                        user = "yelu",
                        password = "yelu123456")
        self._source.connect()
        self._source.binlog_dump("mysql-bin.000299", 406207379)

    def tearDown(self):
        self._source.disconnect()

    def testName(self):
        for i in self._source:
            #time.sleep(1)
            pass

class Test_get_columns_map(unittest.TestCase):

    def setUp(self):
        self._source = source.Source(host = "10.48.78.23",
                        port = 5858,
                        user = "yelu",
                        password = "yelu123456")
        self._source.connect()
        self._source.add_table("FC_Word", "wordinfo0", ["userid", "winfoid"])
        self._source.add_table("FC_Word", "unitinfo0", ["userid", "unitid"])
        
    def tearDown(self):
        self._source.disconnect()

    def testName(self):
        self._source.get_column_map()

'''         
        
class Test_table_map_event(unittest.TestCase):

    def setUp(self):
        self._source = source.Source(host = "10.48.78.23",
                        port = 5858,
                        user = "yelu",
                        password = "yelu123456")
        self._source.connect()
        self._source.binlog_dump("mysql-bin.000400", 4)
        
    def tearDown(self):
        self._source.disconnect()

    def testName(self):
        for i in self._source:
            time.sleep(1)
            pass        
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
