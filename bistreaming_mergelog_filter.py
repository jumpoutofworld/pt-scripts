#!/usr/bin/env python
#coding:gbk
# Author:  pengtao --<pengtao@baidu.com>
# Purpose: 
#     1. 将mergelog根据sid信息过滤，多路输出为不同的part。
# History:
#     1. 2013/2/6


import sys
import struct
import re
from optparse import OptionParser
from string import ascii_uppercase

import log_parser

re_sid = re.compile(r"rsv_sid=([\d_]+)")
packer = struct.Struct("=I")

#----------------------------------------------------------------------
def _flat2dict(fn, sep="\t", override=True):
    """
    Create a memory dict structure from flat file.
    From : key val ==> d[key] = val(None)

    The parameters are less but with the same meaning with function 
    create_dict_from_flat, which, with nest-structured value, will consume a 
    large amount of memory when key number increase.  

    """
    res = {}
    if type(fn) == type(""):
        fn = [fn]
    for f in fn:
        fh = file(f)
        while True:
            line = fh.readline()
            if not line:
                break
            fs = line.strip().split(sep, 1)
            k, v = fs[0], None
            if len(fs) == 2:
                v = fs[1]            
            if k not in res:
                res[k] = v
            else:
                if override:
                    res[k] = v
        fh.close()
    
    return res

#----------------------------------------------------------------------
def parse_args():
    """
    
    """
    parser = OptionParser(usage="usage: %prog [options]. CURRENTLY is not fully implemented")
    parser.add_option("-s","--sid", default=None, help="sid list. ',' as seperator: --sid=2950,2951,3001 " )
    parser.add_option("-q","--query", default=None, help="query dict. None by default " )
    parser.add_option("-c","--cookie", default=None, help="cookie dict. None by default " )
    parser.add_option("-d","--qid", default=None, help="qid dict. None by default " )
    parser.add_option("-l","--logic", default="qid and cookie and query", help="logical operation for filtering. e.g. qid or (cookie and query).  default = 'qid and cookie and query'" )
    options, args = parser.parse_args()
    options.sid = str(options.sid).split(",")
    return options


def get_pb_with_suffix(rec, char):
    """ rec.getPb() 是rec的接口，输出 bistreaming结构
        keylen + key[NULL] + valuelen + value  
        
        函数添加后缀： 
        keylen(32) + key[cookie] + valuelen(+2) + value(#A)
        
    """
    global packer
    buf = rec.getPb()
    
    keylen_str = buf[:4]
    keylen, = packer.unpack(keylen_str)
    keyend = 4+keylen
    key_str = buf[4:keyend]
    
    valuelen_str = buf[keyend:keyend+4]
    valuelen, = packer.unpack(valuelen_str)
    if valuelen != len(buf) - (keyend+4):
        raise ValueError("invalid record %s" % rec.attr("cookie"))
    
    valuelen += 2
    key_str = rec.attr("cookie")
    keylen = len(key_str)
    return packer.pack(keylen) + key_str + packer.pack(valuelen) + buf[keyend+4:] + "#%c" % char
    

#----------------------------------------------------------------------
def get_reducer_suffix(rec, sids, querydict={}):
    """
    if rec in sids, return suffix, else return None
    不在同一层上的sid可能同时命中，会输出多次。        
    cookie可能同时中sid或者不中，因为模版原因（tn）。所以会遍历所有search，只要一次命，cookie则命中。
    
    如果querydict不为{}，则session数据中必须有一个query命中dict文件
    
    return ["A", "C"]
    """
    n = len(sids)
    sid_hit_bits = [0]*n
    query_hit = 0
    if not querydict: # no constraint
        query_hit = 1
    for mission in rec.attr('missions') :
        for goal in mission.attr('goals') :
            for search in goal.attr('searches'):
                if query_hit == 0:
                    query = search.attr("query_info.query")
                    if query in querydict:
                        query_hit = 1
                tp = search.attr("actions_info")[0].attr("tp")
                m = re_sid.search(tp)
                if m:
                    sid_str = m.group(1)
                    sid_list = sid_str.split("_")
                    for s in sid_list:
                        for i in range(n):
                            if s == sids[i]:
                                sid_hit_bits[i] = 1
    suffix = []
    if query_hit == 0:
        return suffix
    for i in range(n):
        if sid_hit_bits[i] == 1:
            suffix.append(ascii_uppercase[i])
    return suffix    
            

#----------------------------------------------------------------------
def filter_mergelog(rec, sids=[], cookie=None, query=None, qid=None, expression=None):
    """
    根据expression，和cookie，query，qid词典，过滤mergelog，目前没有完全实现
    TODO
    """
    c = rec.attr("cookie")
    if cookie and c not in cookie:
        return False
    
    return True
    
            


if __name__=='__main__':
    options = parse_args()
    sids = options.sid
    qf = options.query
    cf = options.cookie
    df = options.qid
    logic = options.logic
    
    cdict = {}
    qdict = {}
    ddict = {}
    if qf is not None:
        qdict = _flat2dict(qf)
    if cf is not None:
        cdict = _flat2dict(cf)
    if df is not None:
        ddict = _flat2dict(df)
    
    input_stream = sys.stdin
    output_stream = sys.stdout
    # input_stream = file("mergelog_part_100")
    # output_stream = file("temp_mergelog_splitter", "w")
    rec = log_parser.MergeLog_Protobuf()

    while True:
        flag = rec.readNext(input_stream)
        if flag <= 0:
            break
        if filter_mergelog(rec, cookie=cdict):
            output_stream.write(get_pb_with_suffix(rec, 'A'))
        #suffix = get_reducer_suffix(rec, sids, qdict)
        #for char in suffix:
            ## print >> sys.stderr, rec.attr("cookie"), "\t", char
            #output_stream.write(get_pb_with_suffix(rec, char))
        
    