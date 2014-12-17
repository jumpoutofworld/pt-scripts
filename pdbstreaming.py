#! /usr/bin/env python
#coding:gbk

"""
 Author:  pengtao --<pengtao@baidu.com>
 Purpose: 
     1. debug hadoop streaming mapper/reducer文件。
     2. 将脚本的input和output进行重定向，避免干扰pdb的交互。
 History:
     1. 2013/7/22 
"""



import sys
import os
from argparse import ArgumentParser


#----------------------------------------------------------------------
def parse_args():
    """
    
    """
    parser = ArgumentParser(description='pdb the hadoop streaming scripts.')
    parser.add_argument("input", metavar='INPUT', help="the input file for streaming script.")
    parser.add_argument("output", metavar='OUTPUT', help="the output file for streaming script.")
    parser.add_argument("script", metavar='SCRIPT', help="the streaming script.")
    parser.add_argument("others", metavar='[others]', nargs='*', help="other arguments. Pass to the script directly.")
    # pdbstreaming.py input output script.py -c aa -d bbb
    # parse_args不处理-c/-d, 会报错
    others = sys.argv[4:]
    sys.argv = sys.argv[:4]
    args = parser.parse_args()
    args.others = others
    return args


#----------------------------------------------------------------------
def pdb_main():
    """This pdb_main is modified from pdb.main().
    """
    from pdb import Pdb, Restart, traceback
    
    mainpyfile =  sys.argv[0]     # Get script filename
    if not os.path.exists(mainpyfile):
        print 'Error:', mainpyfile, 'does not exist'
        sys.exit(1)

    # Replace pdb's dir with script's dir in front of module search path.
    sys.path[0] = os.path.dirname(mainpyfile)

    # Note on saving/restoring sys.argv: it's a good idea when sys.argv was
    # modified by the script being debugged. It's a bad idea when it was
    # changed by the user from the command line. There is a "restart" command
    # which allows explicit specification of command line arguments.
    pdb = Pdb(stdin=sys.__stdin__, stdout=sys.__stdout__)
    while True:
        try:
            pdb._runscript(mainpyfile)
            if pdb._user_requested_quit:
                break
            print "The program finished and will be restarted"
        except Restart:
            print "Restarting", mainpyfile, "with arguments:"
            print "\t" + " ".join(sys.argv[1:])
        except SystemExit:
            # In most cases SystemExit does not warrant a post-mortem session.
            print "The program exited via sys.exit(). Exit status: ",
            print sys.exc_info()[1]
        except:
            traceback.print_exc()
            print "Uncaught exception. Entering post mortem debugging"
            print "Running 'cont' or 'step' will restart the program"
            t = sys.exc_info()[2]
            pdb.interaction(None, t)
            print "Post mortem debugger finished. The " + mainpyfile + \
                  " will be restarted"    

if __name__=='__main__':
    args = parse_args()
    
    # redirect sys.stdin/stdout
    stdin = sys.stdin
    stdout = sys.stdout
    sys.stdin = open(args.input)
    sys.stdout = open(args.output, "w")
    
    sys.argv = [args.script] + args.others
    pdb_main()

    
    
    
    
    
