#!/usr/bin/env python
#coding:gbk

"""
 Author:  pengtao --<pengtao@baidu.com>
 Purpose: 
     1. 将（一行）json代码dump为更可读的形式。
     2. 更简单的方法是利用python -mjson.tool. 有几个问题
         1. 不能处理多行
         2. json.dump()会对编码进行unicode_escape
                 1. "\u4e3b\u8981\u6f14\u51fa\u6d3b\u52a8": 0, 
 History:
     1. 2013/6/9 
"""



import sys
from argparse import ArgumentParser
import json



#----------------------------------------------------------------------
def parse_args():
    """
    
    """
    parser = ArgumentParser(description='beautify json code.')
    parser.add_argument("-i", "--input", default=None, help="input path. default is sys.stdin. ")
    parser.add_argument("-o", "--output", default=None, help="output path. default is sys.stdout. ")
    parser.add_argument("-l", "--line", action="store_true", help="parse multiple records line by line if true. ")
    # parser.add_argument("-u", "--escaple", action="store_true", help="unicode_escape if true. ") 

    args = parser.parse_args()
    if args.input is not None:
        args.input = file(args.input, 'rb')
    else:
        args.input = sys.stdin
    if args.output is not None:
        args.output = file(args.output, "wb")
    else:
        args.output = sys.stdout 
    
    return args

if __name__=='__main__':
    args = parse_args()
    
    ifh = args.input
    ofh = args.output
    
    if args.line:
        for jsonline in ifh:
            try:
                rec = json.loads(jsonline)
                print >> ofh, json.dumps(rec, sort_keys=True, indent=4).decode("unicode_escape").encode("utf-8")
            except ValueError, e:
                raise SystemExit(e)
        ofh.write("\n")
    else:
        try:
            rec = json.load(ifh)
            json.dump(rec, ofh, sort_keys=True, indent=4)
            ofh.write("\n")
        except ValueError, e:
            raise SystemExit(e)

    ifh.close()
    ofh.close()
    
    