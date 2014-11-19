#! /usr/bin/env python
#coding:utf8

"""
Author:  pengtao --<taopeng@meilishuo.com>
Purpose: 
     1. 记住最近进入的绝对路径，方便下次快捷进入
     2. 在python里做不了这件事，改变不了父shell的环境， 需要shell函数 http://linuxgazette.net/109/marinov.html
History:
     1. 2014/11/19 create

"""

import sys
import re

from argparse import ArgumentParser


conf = 

#----------------------------------------------------------------------
def parse_args():
    """

    """
    parser = ArgumentParser(description="shortcut cd for recent paths.")
    parser.add_argument("length", metavar='LENGTH', type=int, help="the length of random string.")
    parser.add_argument("--num", metavar='NUM', type=int, default=1, help="number of required strings.")
    args = parser.parse_args()
    return args


#----------------------------------------------------------------------
def main(args):
    """    """
    if args.list:
        

if __name__=='__main__':
    args = parse_args()
    main(args)

