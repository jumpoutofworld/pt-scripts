#!/usr/bin/env python
#coding:utf8

"""
 Author:  pengtao --<taopeng@meilishuo.com>
 Purpose: 
     1. 下载hadoop上的hive文件part
 History:
     1. 2014/6/26 
"""

import sys
import os
import datetime
from argparse import ArgumentParser


# overwrite hadoop_bin

hadoop_bin = {'default': 'hadoop',
              # , 'rank': '/home/work/hadoop-client-rank/hadoop/bin/hadoop'
              }

logpath = {}
logpath['default'] = {
    'mobile_app_log_new': '/user/hive/warehouse/mobile_app_log_new/dt=%(date)s/vhour=%(vhour)s',
                      }

yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
default_date = yesterday.strftime("%Y-%m-%d")



#----------------------------------------------------------------------
def parse_args():
    """
    
    """
    parser = ArgumentParser(description='download mls log sample from HDFS.')
    parser.add_argument("logname", metavar='LOG', 
                        help="the name of the log. Includes: %s" % logpath['default'].keys()
                        )
    parser.add_argument("-d", "--date", default=default_date, help="date partition of the hive file sample. default=%s (yesterday)" % default_date)
    parser.add_argument("-u", "--vhour", default="bj_100", help="vhour partiton of the log sample, like bj_100, doota_gz_21. default is bj_100")
    parser.add_argument("-t", "--test", action="store_true", help="ls and print the target part name")
    parser.add_argument("-o", "--output", default='./', help="output path. default is './'.")
    args = parser.parse_args()
    return args

if __name__=='__main__':
    args = parse_args()
    hadoop = hadoop_bin['default']
    target_path = logpath['default'][args.logname] % {"date":args.date, "vhour":args.vhour}
    print "target path: ", target_path
    output_path = args.output
    get = '-get'                        # copySeqFileToLocal/orc etc
    cmds = " ".join([hadoop, "dfs", get, target_path, output_path])
    if not args.test:
        os.system(cmds)
