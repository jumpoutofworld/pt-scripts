#!/usr/bin/env python
#coding:gbk
# Author:  pengtao --<pengtao@baidu.com>
# Purpose: 
#     1. pick up random lines (mostly query) from a population file
# History:
#     1. 2012/12/6 update from old version


import math
import sys
import random
import os
import string
from optparse import OptionParser


########################################################################
class CPickRandomLine:
    """pick random line from files
       a simple tool to sample the data
       
       usage:
          >>> p = CPickRandomLine(['input1.txt', 'input2.txt'])
          
          >>> p.set_output_stream(file('output1', "w"))
          >>> p.pick_exact_lines(100)

          >>> p.get_file_sizes() # set_file_sizes()
          >>> p.set_output_stream(file("output2", "w"))
          >>> p.pick_mol_lines(0.13)          
          >>> p.set_output_stream(file("output3", "w"))
          >>> p.pick_mol_lines(300)          
          
       
       """

    #----------------------------------------------------------------------
    def __init__(self, files):
        """sizes is the line numbers for each file"""
        self.nFile = len(files)
        self.Files = files
        random.seed(os.urandom(4))
        self.FileSizes = None
        self.outputstream = None
        
    #----------------------------------------------------------------------
    def set_file_sizes(self, sizes):
        """manually set the file sizes other than get_file_sizes.
        It's useful when the files are really large.
        """
        if self.nFile != len(sizes):
            sys.stderr.write("length of files and sizes do not match %s : %s\n" % (self.nFile, len(sizes)))
            sys.exit(1)
            
        self.FileSizes = sizes
        
    #----------------------------------------------------------------------
    def get_file_sizes(self):
        """get the file sizes by reading file"""
        sizes = [0] * self.nFile
        for i in range(self.nFile):
            fh = file(self.Files[i])
            for l in fh:
                sizes[i] += 1
            fh.close()
        self.FileSizes = sizes
        
    #----------------------------------------------------------------------
    def set_output_stream(self, out):
        """set the output stream for writing"""
        self.outputstream = out
    
    #----------------------------------------------------------------------
    def pick_mol_lines(self, p):
        """sample p lines from files
        p is treated as a portion if p <= 1
        p is treated as a number if p > 1
        Note the simplest strategy is employed. the returned result is not exact p lines
        mol means 'more or less' :-)
        """
        
        pr = 0
        if p <= 0:
            sys.stderr.write("wrong portion/number %f for picking random line\n", float(f))
            sys.exit(1)
        elif p >= 1:
            pr = float(p) / sum(self.FileSizes)
        else:
            pr = p
            
        for fn in self.Files:
            fh = file(fn)
            for l in fh:
                if random.random() < pr:
                    self.outputstream.write(l)
            fh.close()
    
    #----------------------------------------------------------------------
    def pick_exact_lines(self, num):
        """
        Pick exact num lines from files. 
        It will load all file in memory. ONLY for small files. 
        """
        data = []
        for fn in self.Files:
            fh = file(fn)
            for l in fh:
                data.append((l, random.random()))
            fh.close()
        data.sort(key=lambda x: x[1])
        if len(data) < num:
            print >> sys.stderr, "all lines are %d, less than required %d" % (len(data), num)
            num = len(data)
            
        for i in range(num):
            self.outputstream.write(data[i][0])
            
        return 
        
        
            
#----------------------------------------------------------------------
def parse_args():
    """
    
    """
    parser = OptionParser(usage="usage: %prog [options] [ inputfiles [..] ]")
    parser.add_option("-o","--output", default="STDOUT", help="output path. default is 'STDOUT'. " )
    parser.add_option("-n","--num", type="float", help="num of lines to pick up. <1 means a ratio and >=1 means a number" )
    parser.add_option("-t","--type", default="mol", help="type to pick: 'exact' or 'mol' (more or less). default is 'mol'. When num is a ratio, the program will pick only in a mol way." )    
    options, args = parser.parse_args()
    if not args or options.num is None:
        parser.print_help()
        sys.exit(0)
    if options.output == "STDOUT":
        options.output = sys.stdout
    else:
        options.output = open(options.output, "w")
    return (options, args)

if __name__=='__main__':
    (options, args) = parse_args()    

    fileNames = args
    p = CPickRandomLine(fileNames)
    p.set_output_stream(options.output)
    
    if options.num >= 1 and options.type == "exact":
        p.pick_exact_lines(int(options.num))
    else:
        p.get_file_sizes()
        p.pick_mol_lines(options.num)
    
    
      
    
