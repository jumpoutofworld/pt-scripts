#!/usr/bin/env python
# coding:utf-8
# Author:  pengtao --<pengtao@baidu.com>
# Purpose: 
#     1. pick up random lines (mostly query) from a population file
# History:
#     2. 2014/12/17 复杂的抽样，unique 类似的文件
#     1. 2012/12/6 update from old version


import math
import sys
import random
import os
import string
from argparse import ArgumentParser

########################################################################
class CPickRandomLineWithCount:
    """pick random line from files with count number
       a simple tool to sample the data
       
       usage:
          >>> p = CPickRandomLineWithCount(['input1.txt', 'input2.txt'])
          
          >>> p.set_output_stream(file('output1', "w"))
          >>> p.get_total_count() # set_file_sizes()
          >>> p.pick_random_lines()
       
       """

    #----------------------------------------------------------------------
    def __init__(self, fn, ft="0,1", sep="\t"):
        """ """
        self.fn = fn
        random.seed(os.urandom(4))
        self.output_stream = None
        self.total_count = None
        self.ft = map(int, ft.split(","))
        self.sep = sep
        self.fields = []
        self.cumsum_ratio = []
        self.fields_num = None

    #----------------------------------------------------------------------
    def get_total_count(self):
        """get the total count from file"""
        self.total_count = 0
        self.fields = []
        self.cumsum_ratio = []
        for line in open(self.fn):
            fields = line.split(self.sep)
            val, count = map(lambda x: fields[x], self.ft)
            self.total_count += int(count)
            self.fields.append(val)
            self.cumsum_ratio.append(self.total_count)

        total = float(self.total_count)
        self.cumsum_ratio = map(lambda x: x/total, self.cumsum_ratio)
        self.fields_num = len(self.fields)


        
    #----------------------------------------------------------------------
    def set_output_stream(self, out):
        """set the output stream for writing"""
        self.output_stream = out
    
    #----------------------------------------------------------------------
    def pick_random_lines(self, num):
        """
        Pick exact num lines from files. 
        It will load all file in memory. ONLY for small files. 
        """
        data = []
        seeds = map(lambda x: random.random(), range(num))
        seeds.sort()
        idx = 0
        for seed in seeds:
            if seed <= self.cumsum_ratio[idx]:
                data.append(self.fields[idx])
            else:
                while self.cumsum_ratio[idx] < seed:
                    idx += 1
                    if idx == self.fields_num:
                        break
                if idx < self.fields_num:
                    data.append(self.fields[idx])
                else:
                    print >> sys.stderr, "random seed %s is beyond scope where cumsum_ratio[%s]=%s" % (seed, idx, self.cumsum_ratio[idx])


        for ele in data:
            self.output_stream.write(ele + "\n")
            
        return 
        
        
            
#----------------------------------------------------------------------
def parse_args():
    """
    
    """
    parser = ArgumentParser(description="sampling from file with line count.")
    parser.add_argument("input", metavar='INPUT', help="the input file name.")
    parser.add_argument("--output", default="STDOUT", help="output path. default is 'STDOUT'. ")
    parser.add_argument("--num", type=int, default=1000, help="integer num of lines to pick up.")
    parser.add_argument("--sep", default="\t", help="separator in file fields.")
    parser.add_argument("--format", default="kv", help="format of input file: 3,2 means element in field 3 and count in field 2. 'kv' (default) is alias of 0,1 and 'vk' is alias of 1,0 (output of uniq -c)'" )
    options = parser.parse_args()

    if options.output == "STDOUT":
        options.output = sys.stdout
    else:
        options.output = open(options.output, "w")
    if options.format == "kv":
        options.format = "0,1"
    elif options.format == "vk":
        options.format = "1,0"
    return options

if __name__=='__main__':
    args = parse_args()

    p = CPickRandomLineWithCount(args.input, args.format, args.sep)
    p.set_output_stream(args.output)
    p.get_total_count()
    p.pick_random_lines(args.num)
    
    
      
    
