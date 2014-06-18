#!/usr/bin/env python
#coding:gbk
# Author:  pengtao --<pengtao@baidu.com>
# Purpose: 
#     1. pick up random lines by key hash (mostly cookie) from a population file
# History:
#     1. 2014/01/05 created from pick_randome_line.py


import sys
from argparse import ArgumentParser
from hashlib import md5
from struct import unpack
from functools import partial

def get_reducer(key, type):
    """
    hadoop分配reducer的算法见：
        http://wiki.babel.baidu.com/twiki/bin/view/Ps/Rank/UbsTopic/Hadoop
    注意：
        1. hash函数中，c引用key的buffer内容 int(*query). 值域在[-128, 127], 其中query是字符指针
        2. python char2int的函数是ord，但值域[0,255]. 所以计算中文query，两者结果差异很大。这里使用了struct.unpack函数。
            2.1 详见：http://stackoverflow.com/questions/15334465/how-to-map-characters-to-integer-range-128-127-in-python
        
    @type key: string 
    @param key: key to be partitioned
    @type total: int
    @param total: number of reducers
    @type type: int
    @param type: 0-hadoopmap, 1-keyfieldbasedpartitioner
    @rtype: int
    @return: the id of reducer in [0, total)
    
    """
    h = 1 - type
    for c in key:
        # h = 31 * h + ord(c)
        h = 31 * h + unpack("b",c)[0]

    # 2147483647 = 0b11111111111111111111 (31-bit)
    return (h & 2147483647)
        

def split_md5(key):
    """
    md5 hash
    """
    s = md5(key).hexdigest()
    return int(s, 16)
    

def split_key(key):
    """
    keyfieldbasedpartitioner hash
    """
    return get_reducer(key, type=1)


def split_map(key):
    """
    hashmap hash
    """
    return get_reducer(key, type=0)
    
            
#----------------------------------------------------------------------
def parse_args():
    """
    
    """
    parser = ArgumentParser(description='random pick line from a population file by hashing the key.')

    parser.add_argument("denominator", metavar='DEN', type=int,  help="Denominator, the number of parts to divide. ")
    parser.add_argument("file", metavar='FILE', help="file name, the file where samples are picked from. ")
    # parser.add_argument("-n ", "--numerator", default=1, help="Numerator, the number of parts to pick. default is 1. ")
    parser.add_argument("-s", "--sample", default="1", help="sample index list. '1,7,11' means picking the 1st, 7th, 11th from Denominator parts. default is 1. ")
    parser.add_argument("-m", "--mode", default="md5", help="the hash algorithm. md5 = python hashlib.md5; key = KeyFieldBasedPartitioner; map = hashmap. default is md5. ")

    args = parser.parse_args()
    if args.mode not in ("md5", "key", "map"):
        parser.print_help()
    args.sample = map(int, args.sample.split(","))
    return args   


if __name__=='__main__':
    args = parse_args()    

    den = args.denominator
    slist = args.sample
    hash_func = globals()["split_"+args.mode]
    
    for line in file(args.file):
        fs = line.split("\t", 1)
        k = fs[0]
        residual = hash_func(k) % den
        if residual in slist:
            print line,
    
    
      
    
