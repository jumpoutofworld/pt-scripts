#!/usr/bin/env python
#coding:utf8
# Author:  pengtao --<pengtao@baidu.com>
# Purpose: 
#     1. 根据key，获得各种日志对应的part
# History:
#     1. 2013/5/18 


import sys
import os

from ptshell.router import router
from ptutils.hadoop.findreducer import hadoopMap, KeyFieldBasedPartitioner


@router("map")
#----------------------------------------------------------------------
def find_reducer_hadoopMap(k, n):
    """  """
    print hadoopMap(k, int(n))
    
@router("key")
#----------------------------------------------------------------------
def find_reducer_keyfieldbasedpartitioner(k, n):
    """ """
    print KeyFieldBasedPartitioner(k, int(n))

if __name__=='__main__':
    router.main()
    
