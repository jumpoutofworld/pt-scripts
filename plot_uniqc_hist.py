#! /usr/bin/env python
#coding:utf8

"""
 Author:  tao peng --<taopeng@meilishuo.com>
 Purpose:
     1. plot the histogram of count numbers, usuallly by "uinq -c"
 History:
     1. 14-5-22 下午1:04 : plot_uniqc_hist.py is created.
"""



import sys
import os
from argparse import ArgumentParser
# from collections import Counter
import matplotlib.pyplot as plt

from ptutils.math import median


#----------------------------------------------------------------------
def parse_args():
    """
    """
    parser = ArgumentParser(description='plot the histogram of count numbers, usually by "uinq -c".')
    parser.add_argument("file", metavar='FILE', help="input file name.")
    parser.add_argument("max", metavar='MAX', nargs='?', type=int,
                        default=-1, help="threshold for count. default -1 means no threshold.")
    parser.add_argument("bins", metavar='BINS', nargs='?', type=int,
                        default=50, help="number of bins for plotting.")
    parser.add_argument("--column", metavar='COLUMN', type=int,
                        default=0, help="the column of count numbers, default 0.")

    # parser.add_argument("--sep", metavar='SEP', default="\t", help="separator char, default \\t.")
    args = parser.parse_args()
    return args


#----------------------------------------------------------------------
def main(args):
    """

    """
    # sep = args.sep
    col = args.column
    limit = args.max
    # res = Counter()
    res = []
    for line in file(args.file):
        fields = line.strip().split()
        # res[fields[col]] += 1
        v = int(fields[col])
        res.append(v)

    print "median of the %d counts is %s" % (len(res), median(res))
    print "average of the %d counts is %s" % (len(res), sum(res)/len(res))
    sys.stdout.flush()

    if limit != -1:
        res = map(lambda x: x if x < limit else limit, res)

    plt.hist(res, bins=args.bins)
    plt.show()





if __name__=='__main__':
    args = parse_args()
    main(args)
