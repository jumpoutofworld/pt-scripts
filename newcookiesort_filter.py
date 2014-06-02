#!/usr/bin/env python
#coding:gbk
# Author:  pengtao --<pengtao@baidu.com>
# Purpose: 
#     1. 将newcookiesort根据各种信息过滤，多路输出为不同的part。
# History:
#     1. 2013/08/16


import sys
import re
re_sid = re.compile(r"rsv_sid=([\d_]+)")
from ubsutils.filesplitter import split_file_by_ender
from ubsutils.parser.utils import get_rsv_list

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


def output_session(session, fh):
    """
    """
    print >> fh, chr(0).join(map(lambda x: "\t".join(x), session))
                             

if __name__=='__main__':
    fh = sys.stdin
    
    querydict = _flat2dict(sys.argv[1])
    
    gsid = ["2939", "2940"]
    
    for session in split_file_by_ender(fh, "\t"):
        line = session[0]
        cookie = line[0]
        rsv = get_rsv_list(line[12])
        sids = rsv.get("rsv_sid", "").split("_")
        if "2939" not in sids and "2940" not in sids:
            continue
        tag = False
        for line in session:
            query = line[16]
            if query in querydict:
                tag = True
                break
        if tag:
            output_session(session, fh=sys.stdout)
            
        
    
            
        
        
    