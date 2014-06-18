#! /usr/bin/env python
#coding:utf8

"""
 Author:  tao peng --<taopeng@meilishuo.com>
 Purpose:
     1. 生成随机字符串
 History:
     1. 2014/6/17 11:10 : random_string.py is created.
"""



import sys
import random
from argparse import ArgumentParser


#----------------------------------------------------------------------
def parse_args():
    """

    """
    parser = ArgumentParser(description='generate random string.')
    parser.add_argument("length", metavar='LENGTH', type=int, help="the length of random string.")
    parser.add_argument("--num", metavar='NUM', type=int, default=1, help="number of required strings.")
    args = parser.parse_args()
    return args


#----------------------------------------------------------------------
def main(args):
    """    """
    table = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    for i in range(args.num):
        j = 0
        s = ''
        while j < args.length:
            s += table[random.randint(0,61)]
            j += 1
        print s



if __name__=='__main__':
    args = parse_args()
    main(args)

