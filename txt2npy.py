#!/usr/bin/env python
#coding:utf8

"""
 Author:  tao peng --<taopeng@meilishuo.com>
 Purpose:
     1. 将txt文件转换为numpy的npy文件，保证快速加载。
 History:
     1. 2014/6/22 22:57 : txt2npy.py is created.
"""



import sys
import os
import numpy as np
from argparse import ArgumentParser


def np_iter_loadtxt(filename, delimiter=' ', skiprows=0, dtype=float):
    """
    np.genfromtxt很耗内存， 68.9w数据不能执行。
    np.loadtxt略好， 占用接近80%的内存后完成load
    这里是一个更高效的实现。
    http://stackoverflow.com/questions/8956832/python-out-of-memory-on-large-csv-file-numpy
    4.6G的数据， np.loadtxt需要7分多钟，80%内存。 np_iter_loadtxt只需要20%内存， 4分钟。
    """
    def iter_func():
        with open(filename, 'r') as infile:
            for _ in range(skiprows):
                next(infile)
            for line in infile:
                line = line.rstrip().split(delimiter)
                for item in line:
                    yield dtype(item)
        np_iter_loadtxt.rowlength = len(line)

    data = np.fromiter(iter_func(), dtype=dtype)
    data = data.reshape((-1, np_iter_loadtxt.rowlength))
    return data

#----------------------------------------------------------------------
def parse_args():
    """

    """
    parser = ArgumentParser(description='convert txt matrix into numpy npy binary format.')
    parser.add_argument("input", metavar='INPUT', help="the input txt matrix.")
    parser.add_argument("output", metavar='OUTPUT', help="the output npy binary file.")
    # parser.add_argument("row", metavar='ROW', type=int, help="number of rows.")
    # parser.add_argument("col", metavar='COL', type=int, help="number of column.")
    parser.add_argument("--type", metavar='TYPE', nargs='?', default="float", help="element type: float or int.")
    args = parser.parse_args()
    if args.type not in ('float', 'int'):
        print >> sys.stderr, 'type must be float or int'
        parser.print_help()
    else:
        if args.type == "float":
            args.type = float
        elif args.type == 'int':
            args.type = int
    return args


#----------------------------------------------------------------------
def main(args):
    """
    """
    # data = np.loadtxt(args.input, dtype=args.type)
    data = np_iter_loadtxt(args.input, dtype=args.type)
    np.save(args.output, data)

if __name__=='__main__':
    args = parse_args()
    main(args)

