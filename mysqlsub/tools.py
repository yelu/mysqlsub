#!/usr/bin/env python
#coding:gbk

import os
import sys
import logging
import traceback

# config log
if not os.path.exists("./log"):
    os.makedirs("./log")
logger = logging.getLogger()
hdlr = logging.StreamHandler(sys.stderr)
formatter = logging.Formatter('%(levelname)s: %(asctime)s:  %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.DEBUG)

class log: 
    @staticmethod
    def debug(msg):
        global logger
        logger.debug(msg)
      
    @staticmethod
    def info(msg):
        global logger
        logger.info(msg)

    @staticmethod
    def warning(msg):
        global logger
        logger.warning(msg)
    
    @staticmethod
    def fatal(msg):
        global logger
        logger.critical(msg)

class chdir_temp:
    def __init__(self, dst):  
        self.dst = dst
        self.cwd = os.getcwd()
    def __enter__(self):
        os.chdir(self.dst)
        return self.cwd
    def __exit__(self, type, value, traceback):
        os.chdir(self.cwd)

class open_cursor:
    def __init__(self, conn):  
        self.cursor = conn.cursor()
    def __enter__(self):
        return self.cursor
    def __exit__(self, type, value, traceback):
        self.cursor.close()

def join_path(start_path, relative):
    if len(relative) < 1:
        raise RuntimeError
    if relative[0] == '/':
        return relative
    if relative[0] != '.':
        raise RuntimeError
    cwd = os.getcwd()
    os.chdir(start_path)
    abspath = os.path.abspath(relative)
    os.chdir(cwd)
    return abspath

def get_trace_info():
    t, v, tb = sys.exc_info()
    res = traceback.format_exception(t, v, tb)
    return "".join(res)

