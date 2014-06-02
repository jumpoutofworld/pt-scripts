#!/usr/bin/env python
#coding:gbk

"""
 Author:  pengtao --<pengtao@baidu.com>
 Purpose: 
     1. 按照cookie查询联盟和不包含联盟的newcookiesort数据差别
 History:
     1. 2013/6/28 
"""
import subprocess
import os
import sys
from urlparse import urlsplit

from flask import Flask, request, session, g, redirect, url_for, abort, \
     render_template, flash, _app_ctx_stack
import leveldb


# configuration
LEVELDB_NAME_1 = "newcookiesort_organ_db" # 如果LEVELDB_NAME对应的文件夹不存在，那么 LEVELDB_FILE数据会被导入。
LEVELDB_FILE_1 = "../bin/part-00299.sample500.newcookiesort"
LEVELDB_NAME_2 = "newcookiesort_all_db" # 如果LEVELDB_NAME对应的文件夹不存在，那么 LEVELDB_FILE数据会被导入。
LEVELDB_FILE_2 = "../bin/part-00299.sample500.newcookiesort.all"


DEBUG = True
LEVELDB_WRITER = "leveldb_kv_writer"


# newcookiesort 字段映射
IDX = {
    "cookie":0,
    "ip":1,
    "time":2,
    "fm":3,
    "pn":4,
    "p1":5,
    "p2":6,
    "p3":7,
    "p4":8,
    "tn":9,
    "tab":10,
    "title":11,
    "tp":12,
    "f":13,
    "rsp":14,
    "F":15,
    "query":16,
    "url":17,
    "BDUSS":18,
    "weight":19,
    "ID":20,
    "info":21,
    "prefixSug":22,
    "mu":23,
    "s":24,
    "oq":25,
    "qid":26,
    "cid":27    
}



app = Flask(__name__)
app.config.from_object(__name__)

_g_db1 = None
_g_db2 = None


def get_db(idx):
    """connect to the database and get the dbcursor       
    """
    global _g_db1, _g_db2
    if _g_db1 is None:
        db_name = app.config["LEVELDB_NAME_1"]
        db_file = app.config["LEVELDB_FILE_1"]
        _g_db1 = _init_db(db_name, db_file)
    if _g_db2 is None:
        db_name = app.config["LEVELDB_NAME_2"]
        db_file = app.config["LEVELDB_FILE_2"]
        _g_db2 = _init_db(db_name, db_file)
    if idx == 1:
        return _g_db1
    elif idx == 2:
        return _g_db2
    elif idx == 0 :
        return (_g_db1, _g_db2)
    else:
        raise Exception


def _init_db(db_name, db_file):
    if not os.path.exists(db_name):
        if os.path.exists(db_file):
            db_kv_file = db_file + ".kv"
            print >> sys.stderr, "convert data %s to kv file %s" % (db_file, db_kv_file)
            _newcookiesort_to_kv_file(db_file, db_kv_file)
            print >> sys.stderr, \
                subprocess.check_output([app.config["LEVELDB_WRITER"], "-i", db_kv_file, "-o", db_name])
            os.remove(db_kv_file)
        else:
            print >> sys.stderr, "leveldb data file %s is missing for %s" % (db_file, db_name)
            sys.exit(1)           
    handle = leveldb.open(db_name)
    db = DBcursor(handle)
    return db
    

def _newcookiesort_to_kv_file(fromfile, tofile):
    """
    ==== from ====
    cookie1 act1
    cookie1 act2
    
    cookie1 act1
    cookie1 act2
    
    ==== to ====
    cookie1 act1{chr(3)}act2{chr(2)}act3{chr(3)}act4
    """
        
    fhi = file(fromfile)
    fho = file(tofile, 'w')
    
    last_cookie = ""
    info = []
    search = []
    for line in fhi:
        fields = line.strip().split("\t")
        if fields[0] != last_cookie:
            if last_cookie:
                print >> fho, last_cookie + "\t" + _serialize(info)
            last_cookie = fields[0]
            search = [fields[1:]]
            info = [search]
        else:
            fm = fields[IDX["fm"]]
            if fm == 'se':
                search = [fields[1:]]
                info.append(search)
            else:
                search.append(fields[1:])
    if info:
        print >> fho, last_cookie, "\t", _serialize(info)
        
    fhi.close()
    fho.close()
    

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
        


def _get_qids(info):
    """"""
    res = set()
    for search in info:
        if search:
            qid = search[0][IDX["qid"]-1]
            res.add(qid)
    return res
    


def _serialize(info):
    """
    """
    res = []
    for search in info:
        tmp = []
        for act in search:
            tmp.append("\t".join(act))
        res.append(chr(3).join(tmp))
    return chr(2).join(res)
    
    


def _deserialize(line):
    res = []
    if not line:
        return res
    for search in line.strip().split(chr(2)):
        tmp = []
        for act in search.strip().split(chr(3)):
            tmp.append(act.strip().split("\t"))
        res.append(tmp)
    return res
    

def session_diff(val1, val2):
    """"""
    actions1 = _deserialize(val1)
    actions2 = _deserialize(val2)
    
    qids1 = _get_qids(actions1)
    qids2 = _get_qids(actions2)
    qboth = qids1.intersection(qids2)
    
    actions2_both = {}
    actions2_uniq = []
    for l in actions2:
        qid = l[0][IDX["qid"]-1]
        if qid in qboth:
            if qid in actions2_both:
                actions2_both[qid].append(l)
            else:
                actions2_both[qid] = [l]
        else:
            actions2_uniq.append(l)

    res_struct = []
    j = 0
    for l1 in actions1:
        # cookie, time, query, tn, rsv_bp, qid
        t = l1[0][IDX["time"]-1]
        while j < len(actions2_uniq):
            l2 = actions2_uniq[j]
            t2 = l2[0][IDX["time"]-1]
            if t2 >= t:
                break
            res_struct.append((None,l2))
            j += 1
        qid = l1[0][IDX["qid"]-1]
        if qid not in actions2_both:
            res_struct = (l1, None)
        else:
            l2s = actions2_both[qid]
            res_struct.append((l1, l2s[0]))
            for l2 in l2s[1:]:
                res_struct.append((None, l2))
    while j < len(actions2_uniq):
        l2 = actions2_uniq[j]
        res_struct.append((None, l2))
        j += 1
        
    return res_struct

def _simplify_seach(search):
    if not search:
        return None
        
    obj = {}
    obj["search"] = {}
    obj["clicks"] = []
    
    obj["search"]["fm"] = search[0][IDX["fm"]-1]
    obj["search"]["pn"] = search[0][IDX["pn"]-1]
    obj["search"]["time"] = search[0][IDX["time"]-1].split(":",1)[1]
    obj["search"]["query"] = search[0][IDX["query"]-1].decode("gbk")
    obj["search"]["tn"] = search[0][IDX["tn"]-1]    
    obj["search"]["f"] = search[0][IDX["f"]-1]
    obj["search"]["qid"] = search[0][IDX["qid"]-1]
    for clk in search[1:]:
        click = {}
        click["fm"] = clk[IDX["fm"]-1]
        click["p1"] = clk[IDX["p1"]-1]
        click["time"] = clk[IDX["time"]-1].split(":",1)[1]
        click["url"] = clk[IDX["url"]-1]        
        click["site"] = urlsplit(clk[IDX["url"]-1]).netloc
        click["title"] = clk[IDX["title"]-1].decode("gbk")
        click["qid"] = clk[IDX["qid"]-1]
        if click["qid"] == obj["search"]["qid"]:
            click["qid"] = ""
        try:
            click["url"] = click["url"].decode("gbk")        
        except UnicodeDecodeError:
            try:
                click["url"] = click["url"].decode("utf8")
            except UnicodeDecodeError:
                pass
            
        obj["clicks"].append(click)

    return obj
        

    
    
    
def simplify_diff_struct(struct):
    """ simplify  the diff struct into a nested dict for rendering"""
    res = []
    for (info1, info2) in struct:
        obj1 = _simplify_seach(info1)
        obj2 = _simplify_seach(info2)
        res.append( (obj1, obj2) )

    return res

@app.route("/")
def index():
    # return "Hello World!"
    return redirect("/searchform")

@app.route("/searchform")
def searchform():
    """"""
    cookie = request.args.get("cookie", "")
    verbose = False
    if "detail" in request.args:
        verbose = True
    if not cookie:
        return render_template('search.html', cookie=cookie)    
    db1 = get_db(1)
    db2 = get_db(2)
    val1 = db1.get(cookie)
    val2 = db2.get(cookie)
    res_struct = session_diff(val1, val2)
    date = 'XX/XX/2014'
    if res_struct:
        s1, s2 = res_struct[0]
        if s1:
            search = s1[0]
        else:
            search = s2[0]
        date = search[IDX['time']-1].split(":",1)[0]
    render_struct = simplify_diff_struct(res_struct)
    return render_template('search.html', res_struct=render_struct, cookie=cookie, date=date, verbose=verbose, IDX=IDX)
    

@app.route("/hello/<name>")
def hello(query="OK"):
    # return "OK"
    return "hello, %s !" % query
    
    

if __name__ == "__main__":
    get_db(0)  # init the data loading if any
    # db1 = get_db(1)
    # val = db1.get("0155865C156EFFB6DA1FAE8F7DCDBB5A")    
    # print val
    # _newcookiesort_to_kv_file(LEVELDB_FILE_1, "tmp.kv")
    # sys.exit(0)

    app.run(host="0.0.0.0", port=8081, \
            # debug mode on, 默认开启一个reloader，代码会被重新执行， leveldb.open 执行两次，再查时会引发段错误。
            use_reloader=False)


    
