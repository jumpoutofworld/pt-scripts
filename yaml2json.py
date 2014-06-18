#! /usr/bin/env python
#coding:utf8

"""
 Author:  tao peng --<taopeng@meilishuo.com>
 Purpose:
     1. 将yaml配置文件转化为json。当前线上php没有yaml的extension。折中办法。
 History:
     1. 2014/6/18 15:40 : yaml2json.py is created.
"""

from argparse import ArgumentParser
import json
import yaml


#----------------------------------------------------------------------
def parse_args():
    """

    """
    parser = ArgumentParser(description='convert yaml file to json.')
    parser.add_argument("input", metavar='INPUT', help="input yaml file name.")
    parser.add_argument("output", metavar='OUTPUT', help="output json file name.")
    args = parser.parse_args()
    return args


#----------------------------------------------------------------------
def main(args):
    """
    """
    obj = yaml.load(open(args.input))
    json.dump(obj, open(args.output, "w"), indent=4)


if __name__=='__main__':
    args = parse_args()
    main(args)

