#! /usr/bin/env python
#coding:gbk

"""
 Author:  pengtao --<pengtao@baidu.com>
 Purpose: 
     1. 通过service获取一个资源(srcid)在不同抽样下的id下的xpath点击详情
     2. 输入: srcid list, sample list, date list
 History:
     1. 2013/8/16 
"""

REQUEST = "http://tc-ps-ubstest0.tc.baidu.com:8062/index.php?username=caodaijun&tool_type=1&sample_id=%(sid)s&aladdin_source_id=%(srcid)s&date=%(date)s&t=1376637740222"


import sys
from argparse import ArgumentParser
from urllib2 import urlopen, HTTPError
import json
from time import sleep


#----------------------------------------------------------------------
def parse_args():
    """
    
    """
    parser = ArgumentParser(description='get the json data of xpath click for a srcid in a sample.')
    parser.add_argument("output", metavar='OUTPUT', help="the output file")
    parser.add_argument("-s", "--sid", help="sample id like '3098,3099'. ")
    parser.add_argument("-c", "--srcid", help="source id like '2011,2012'. ")
    parser.add_argument("-d", "--date", help="date string like '20130806,20130705'. ")
    
    args = parser.parse_args()
    if args.sid is not None:
        args.sid = args.sid.split(",")
    else:
        args.sid = []
    args.srcid = args.srcid.split(",")
    # 20130806 --> 2013-08-06
    args.date = map(lambda x: x[:4]+"-"+x[4:6]+"-"+x[6:8], args.date.split(","))
    return args

if __name__=='__main__':
    args = parse_args()
    
    # 输出格式： { srcid: {sid: {date: {data}}}}
    # http://tc-ps-ubstest0.tc.baidu.com:8062/index.php?username=caodaijun&tool_type=1&sample_id=2776&aladdin_source_id=20112&date=2013-08-14&t=1376637740222
    alldata = {}
    for srcid in args.srcid:
        alldata[srcid] = {}
        for sid in args.sid:
            alldata[srcid][sid] = {}
            for date in args.date:                
                req = REQUEST % {"sid":sid, "srcid":srcid, "date":date}
                try:
                    sz = urlopen(req).read()
                    alldata[srcid][sid][date] = eval(sz)
                    print >> sys.stderr, "get srcid=%s - sid=%s - date=%s ..." % (srcid, sid, date)
                except HTTPError:
                    print >> sys.stderr, "error get srcid=%s - sid=%s - date=%s ..." % (srcid, sid, date)
                sleep(1)

    ofh = open(args.output, "w")                    
    json.dump(alldata, ofh, sort_keys=True, indent=4)
    ofh.close()
      
            
                
                
                