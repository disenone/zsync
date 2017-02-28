# -*- coding: utf-8 -*-

import argparse
import time
import sys
import zmq
import zsync_thread
import zsync_utils
import zhelpers
import subprocess
from collections import deque
import logging
import zsync_process
import time

def sys_excepthook(typ, value, tb):
    import pdb
    import traceback
    traceback.print_exception(typ, value, tb)
    msgs = []
    while tb:
        msg = 'locals: ' + str(tb.tb_frame.f_locals)
        if len(msg) > 2000:
            msg = msg[:2000] + ' Truncated...'
        msgs.append(msg)
        tb = tb.tb_next

    print '\n'.join(msgs)
    pdb.pm()
    return

def prepare_log(debug):
    if debug:
        logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s: %(message)s',
            datefmt='%Y-%m-%d,%H:%M:%S', level=logging.DEBUG)
        sys.excepthook = sys_excepthook
    else:
        logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s: %(message)s',
            datefmt='%Y-%m-%d,%H:%M:%S', level=logging.INFO)
    return

def prepare_args(help=False):
    parser = argparse.ArgumentParser()
    parser.add_argument('--daemon', action='store_true', help='start as daemon server')
    parser.add_argument('--local', action='store_true', help='start as local server')
    parser.add_argument('--remote', action='store_true', help='start as remote server')
    parser.add_argument('src', type=str, default='', nargs='?', help='src path')
    parser.add_argument('dst', type=str, default='', nargs='?', help='dst path')
    parser.add_argument('-p', '--port', type=str, default='5555', help='if local, client port, else daemon port')
    parser.add_argument('--thread-num', type=int, default=3, help='sync thread num')
    parser.add_argument('--timeout', type=int, default=30, help='connect timeout second')
    parser.add_argument('--pipeline', type=int, default=10, help='file fetch pipeline')
    parser.add_argument('--chunksize', type=int, default=262144, help='chunksize for each pipeline')
    parser.add_argument('--exclude', type=str, action='append', help='exclude file or directory to sync, use regular expression')
    parser.add_argument('--compress', action='store_true', help='compress data')
    parser.add_argument('--debug', action='store_true', help='debug mode')

    if help:
        parser.print_help()
        return

    args = parser.parse_args()

    prepare_log(args.debug)

    src = zsync_utils.CommonPath(args.src)
    dst = zsync_utils.CommonPath(args.dst)

    if not src.isValid():
        logging.error('src is invalid')
        return False

    if not dst.isValid():
        logging.error('dst is invalid')
        return False

    if not 0 < args.timeout <= 300:
        logging.error('timeout is invalid, must be in [1, 300]')
        return False

    if not 0 < args.thread_num <= 10:
        logging.error('thread_num is invalid, must be in [1, 10]')
        return False

    if not 0 < args.pipeline <= 20:
        logging.error('pipeline is invalid, must be in [1, 20]')
        return False

    if not 1000 <= args.chunksize <= 1048576:
        logging.error('chunksize is invalid, must be in [1000, 500000]')
        return False

    if not args.daemon:
        if not args.src or not args.dst:
            logging.error('src path or dst path is not specific')
            return False

    args.excludes = args.exclude
    delattr(args, 'exclude')

    try:
        args.src = args.src.decode(sys.stdin.encoding)
        args.dst = args.dst.decode(sys.stdin.encoding)
    except Exception as e:
        logging.critical('path encoding error: ' + str(e))

    try:
        zsync_utils.CommonExclude(args.excludes)
    except Exception as e:
        logging.error('--exclude pattern error: ' + str(e))
        return False

    logging.debug(args)

    return args

# run in different mode
def run(args):
    if args.daemon:
        target = zsync_process.ZsyncDaemon(args)

    elif args.local:
        target = zsync_process.ZsyncLocalService(args)

    elif args.remote:
        target = zsync_process.ZsyncRemoteService(args)

    else:
        target = zsync_process.ZsyncClient(args)
    
    target.run()
    return

def main():
    args = prepare_args()
    if args:
        run(args)
    else:
        prepare_args(help=True)
    return

if __name__ == '__main__':
    begin_time = time.time()
    main()
    end_time = time.time()
    logging.info('cost time: %ss' % (end_time - begin_time))
