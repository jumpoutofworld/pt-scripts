#!/usr/bin/env python
#coding:gbk

"""
 Author:  pengtao --<pengtao@baidu.com>
 Purpose: 
     1. 下载（stoff）上的的日志part
 History:
     1. 2013/6/3 
"""

import sys
import os
import datetime
from argparse import ArgumentParser

from ubsutils.pipe.mr import MRConf
from ubsutils.hadoop.logpath import hadoop_bin, logpath

# overwrite hadoop_bin

hadoop_bin = {'stoff': '/home/users/pengtao/hadoop-client-stoff/hadoop/bin/hadoop', 'rank': '/home/work/hadoop-client-rank/hadoop/bin/hadoop'}

yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
default_date = yesterday.strftime("%Y%m%d")


#----------------------------------------------------------------------
def parse_args():
    """
    
    """
    parser = ArgumentParser(description='download log sample from HDFS.')
    parser.add_argument("logname", metavar='LOG', 
                        help="the name of the log. Includes: %s" % logpath['stoff'].keys()
                        )
    parser.add_argument("-d", "--date", default=default_date, help="date of the log sample. default=%s (yesterday)" % default_date)
    parser.add_argument("-u", "--hour", default="09", help="hour of the log sample")
    parser.add_argument("-m", "--minute", default="00", help="minute of the log sample")
    parser.add_argument("-f", "--machine", default="*", help="machine of the log sample")
    parser.add_argument("-n", "--num", type=int, default=1, help="number of sample parts. default=1")
    parser.add_argument("-p", "--part", default="*", help="index of sample parts. default=*")    
    parser.add_argument("-c", "--cluster", default="stoff", 
                        help="hdfs namenode. default 'stoff'. Includes: %s" % logpath.keys()
                        )
    parser.add_argument("-o", "--output", default=None, help="output file prefix.")
    parser.add_argument("-t", "--test", action="store_true", help="ls and print the target part name")
    args = parser.parse_args()
    if args.part != '*':
        args.part = "%05d" % int(args.part)
    return args

if __name__=='__main__':
    args = parse_args()
    output_prefix = None
    if args.output:
        output_prefix = args.output
    else:
        output_prefix = "_".join(filter(lambda x: x is not None and x != "*", [args.logname, args.date, args.hour, args.minute, args.machine]))

    
    hadoop = hadoop_bin[args.cluster]
    path = logpath[args.cluster][args.logname] % {"date":args.date, "hour":args.hour, "minute":args.minute, "machine":args.machine, "part":args.part}
    print "target path: ", path
    mrconf = MRConf()
    parts = mrconf.get_hdfs_parts(paths=[path], numofparts=args.num, hadoop=hadoop)
    for p in parts:
        basename = os.path.basename(p)
        get = "-get"
        output_name = output_prefix + "_" + basename
        if args.logname == "mergelog":
            get = "-copySeqFileToLocal"
        cmds = " ".join([hadoop, "dfs", get, p, output_name])
        print >> sys.stderr, cmds
        if not args.test:
            os.system(cmds)

