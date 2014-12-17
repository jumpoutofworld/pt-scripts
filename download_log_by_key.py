#!/usr/bin/env python
#coding:gbk
# Author:  pengtao --<pengtao@baidu.com>
# Purpose: 
#     1. 根据key，获得各种日志对应的part
# History:
#     1. 2013/5/18 


import sys
import os

from ubsutils.pipe.router import router
from ubsutils.hadoop.findreducer import hadooMap, KeyFieldBasedPartitioner


            
hadoop_bin = "/home/users/pengtao/hadoop-client-stoff/hadoop/bin/hadoop"        
    
    

@router("dnq")
#----------------------------------------------------------------------
def get_detailnewquerysort_part(k, n=200, date=None, output=None):
    """
    @type k: string
    @param k: key to be partitioned, "123", "query" etc
    @type n: int
    @param n: number of reducers
    @type d: string
    @param d: date string like "20130516"
    @type output: string
    @param output: output file name. part-XXXXX remains if None.
    
    """
    
    n = int(n)
    i = KeyFieldBasedPartitioner(k, n)
    print "rid = %d" % i
    if date:
        fn = "part-%05d" % i
        cmds = [hadoop_bin, "dfs", "-get", "/log/20682/detailnewquerysort/%s/0000/szwg-rank-hdfs.dmop/%s" % (date, fn)]
        if output:
            cmds.append(output)
        else:
            cmds.append("./")

        print >> sys.stderr, " ".join(cmds)
        os.system(" ".join(cmds))
                
    return i
    




@router("oqc")
#----------------------------------------------------------------------
def get_oldquerycube_part(k, n=500, date=None, hour=None, output=None):
    """
    @type k: string
    @param k: key to be partitioned, "123", "query" etc
    @type n: int
    @param n: number of reducers
    @type d: string
    @param d: date string like "20130516"
    @type h: string 
    @param h: hour string like 21
    @type output: string
    @param output: output file name. part-XXXXX remains if None.
    
    """
    n = int(n)
    # key in mapper2reducer is like: {'query': '\xd7\xaf\xba\xd3\xb0\xc9'}
    key = "{'query': %s}" % repr(k)
    i = hadooMap(key, n)
    print "rid = %d" % i
    
    # ans = raw_input("download? Y[es] or N[o]:")
    if date and hour:
        fn = "part-%05d" % i
        cmds = [hadoop_bin, "dfs", "-get", "/ps/ubs/mahao/monitor-target-out-v2/%s/%s/oldquerycube/%s" % (date, hour, fn)]
        if output:
            cmds.append(output)
        else:
            cmds.append("./")

        print >> sys.stderr, " ".join(cmds)
        os.system(" ".join(cmds))

                
    return i


if __name__=='__main__':
    router.main()
    
