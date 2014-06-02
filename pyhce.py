#! /usr/bin/env python
#coding:gbk

"""
Author:  pengtao --<pengtao@baidu.com>
Purpose:
    1. hce官方提供的pyhce本地调试版本
    2. 非常简短，原理如下：
       1. 框架根据参数找到脚本，import为一个对象
       2. 框架读入input，格式化为k，v，多次调用脚本中的mapper，emit（由框架提供，贴到脚本空间中）到框架的collector（一个list对象）
       3. 框架吧collector中的数据排序，依次调用reducer，emit到同一个collector中，写入output文件
History:
    4. 2013-09-30, 将路径改为 ubsutils/scripts, 将pyhce.py放入类似~/local/bin/， 直接用pyhce.py启动调试
    3. 2013-03-16, 集成进入ubsutils，方便 -mubsutils.pyhce调用
    2. 2011/10/28 在环境中添加emit
    1. Created on May 13, 2011 @author: changbinglin(changbinglin@baidu.com)
        pyhce local runner for local testing your pyhce based mapreduce programs
    
"""



__version__ = "0.1.2"

import sys
import os
import itertools
from optparse import OptionParser
import imp


class MRContext(object):
  ''' mapreduce job context '''
  def __init__(self,args, input,output):
    self.nreduce = 1
    self.map_input_file = input
    self.map_output_file = ""
    self.reduce_input_file = ""
    self.reduce_output_file = output
    self.args = args
    self.collector = []
    self.midoutput = None
    self.module = None

  def _prepare_hceutil(self):
    """
    Insert hceutil.emit for sub-module import.
    Make the offline-debug and online-product enviroment consistent.

    """
    modname = 'hceutil'
    mod = imp.new_module(modname)
    sys.modules[modname] = mod
    setattr(mod, 'emit', self.emit)


  def load_script(self):
    # set environ
    if os.getenv('map_input_file') is None:
      os.environ['map_input_file'] = self.map_input_file
    if os.getenv('mapred_task_is_map') is None:
      os.environ['mapred_task_is_map'] = 'true'
    if os.getenv('mapred_reduce_tasks') is None:
      os.environ['mapred_reduce_tasks'] = '1'
    if os.getenv('mapred_task_partition') is None:
      os.environ['mapred_task_partition'] ='0'

    self._prepare_hceutil()

    sys.argv = self.args[:]
    script = self.args[0]
    # # __import__ could not handle a full path to a file
    # name = script[:script.rfind('.')]
    # print "load module %s" % name
    # self.module = __import__(name)
    fn = os.path.basename(script)
    name, ext = os.path.splitext(fn)
    self.module = imp.load_source(name, script)
    # setattr(self.module, "emit", self.emit)

  def run_mapper(self, debug=False):
    # set environ
    os.environ['mapred_task_is_map'] = 'true'
    print "start to run mapper, read form file: %s" % self.map_input_file
    if hasattr(self.module, "mapper_setup"):
      if False == getattr(self.module, "mapper_setup")():
        raise Exception("mapper setup failed")
    if not hasattr(self.module, "mapper"):
      raise Exception("no mapper function found!")
    mapper = getattr(self.module, "mapper")
    if debug:
      import pdb
      pdb.set_trace()      
    for l in open(self.map_input_file,'r'):
      if l[-1] == '\n':
        l = l[:-1]
      mapper('',l)
    if hasattr(self.module, "mapper_cleanup"):
      if False == getattr(self.module, "mapper_cleanup")():
        raise Exception("mapper cleanup failed!")
  def run_shuffle_sort(self):
    self.midoutput = self.collector
    self.collector = []
    print "start to sort, %d kv pairs" % len(self.midoutput)
    self.midoutput.sort(key=lambda x:x[0])
    print "sort finished"
  def run_reducer(self, debug=False):
    # set environ
    os.environ['mapred_task_is_map'] = 'false'
    print "start to run reducer"
    if hasattr(self.module, "reducer_setup"):
      if False == getattr(self.module, "reducer_setup")():
        raise Exception("reducer setup failed")
    if not hasattr(self.module, "reducer"):
      raise Exception("no reducer function found!")
    reducer = getattr(self.module, "reducer")
    if debug:
      import pdb
      pdb.set_trace()    
    for k,vs in itertools.groupby(self.midoutput, lambda x:x[0]):
      rvs = []
      for v in vs:
        rvs.append(v[1])
      reducer(k,rvs)
    if hasattr(self.module, "reducer_cleanup"):
      if False == getattr(self.module, "reducer_cleanup")():
        raise Exception("reducer_cleanup failed!")
  def output(self):
    print "start to dump output to file: %s" % self.reduce_output_file
    out = open(self.reduce_output_file,'w')
    for kv in self.collector:
      if (not kv[0]) and kv[1]:
        out.write(kv[1])
      elif kv[0] and (not kv[1]):
        out.write(kv[0])
      elif kv[0] and kv[1]:
        out.write(kv[0]+"\t"+kv[1])
      out.write('\n')
  def emit(self, k, v):
    self.collector.append( (k,v) )


if __name__ == "__main__":
  parser = OptionParser(usage="usage: %prog [options] <python script> args1 args2 ...")
  parser.add_option("-i","--input",default="input",help="input file path, default ./input")
  parser.add_option("-o","--output",default="output",help="output file path, default ./output")
  parser.add_option("-m","--map",action="store_true", default=False,help="run map only, default False")
  parser.add_option("-p","--pdb",action="store_true", default=False,help="start pdb during the mapper, default False")
  parser.add_option("-r","--rpdb",action="store_true", default=False,help="start pdb during the reducer, default False")  
  
  if len(sys.argv) < 2:
    parser.print_help()
    sys.exit(1)
  options, args = parser.parse_args()
  mapreduce = MRContext(args, options.input, options.output)
  mapreduce.load_script()
  mapreduce.run_mapper(debug=options.pdb)
  if options.map == False:
    mapreduce.run_shuffle_sort()
    mapreduce.run_reducer(debug=options.rpdb)
  mapreduce.output()

