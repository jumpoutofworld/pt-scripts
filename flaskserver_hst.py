#!/usr/bin/env python
#coding:gbk

"""
 Author:  pengtao --<pengtao@baidu.com>
 Purpose: 
     1. leveldb的封装sever
 History:
     1. 2013/6/28 
"""
import subprocess
import os
import sys

from flask import Flask, request, session, g, redirect, url_for, abort, \
     render_template, flash, _app_ctx_stack
import leveldb

# configuration
LEVELDB_NAME = "mytest_db" # 如果LEVELDB_NAME对应的文件夹不存在，那么 LEVELDB_FILE数据会被导入。
LEVELDB_FILE = ""   
DEBUG = True
LEVELDB_WRITER = "leveldb_kv_writer"


app = Flask(__name__)
app.config.from_object(__name__)

_g_db = None

########################################################################
class DBcursor:
    """
    simple wrapper for leveldb handler
    """

    #----------------------------------------------------------------------
    def __init__(self, handle):
        """Constructor"""
        self._handle = handle
    #----------------------------------------------------------------------
    def get(self, key):
        """"""
        return leveldb.get(self._handle, key)
    
    #----------------------------------------------------------------------
    def put(self, key, val):
        """"""
        return leveldb.put(self._handle, key, val)
        
        
        
    
    
def get_db():
    """connect to the database and get the dbcursor       
    """
    global _g_db
    if _g_db is None:
        db_name = app.config["LEVELDB_NAME"]
        db_file = app.config["LEVELDB_FILE"]
        
        if not os.path.exists(db_name):
            if os.path.exists(db_file):
                print >> sys.stderr, "load data %s into leveldb %s" % (db_file, db_name)
                print >> sys.stderr, \
                      subprocess.check_output([app.config["LEVELDB_WRITER"], "-i", db_file, "-o", db_name])
            else:
                print >> sys.stderr, "leveldb data file %s is missing for %s" % (db_file, db_name)
                sys.exit(1)           
        handle = leveldb.open(db_name)
        _g_db = DBcursor(handle)
        
    # print "_g_db=%s" % _g_db    
    return _g_db




@app.route("/")
def index():
    # return "Hello World!"
    return redirect("/search/")

@app.route("/search/")
@app.route("/search/<query>")
def search(query="\"fs2you+gv"):
    """"""
    db = get_db()
    val = db.get(query).decode('utf8')
    return render_template('search.html', query="q=%s v=%s" % (query, val))
    

@app.route("/hello/<name>")
def hello(query="OK"):
    # return "OK"
    return "hello, %s !" % query
    
    

if __name__ == "__main__":
    get_db()  # init the data loading if any
    app.run(host="0.0.0.0", port=8081, \
            # debug mode on, 默认开启一个reloader，代码会被重新执行， leveldb.open 执行两次，再查时会引发段错误。
            use_reloader=False)


    