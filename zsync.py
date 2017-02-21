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


def prepare_log():
    logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d,%H:%M:%S', level=logging.DEBUG)
    return

def prepare_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--daemon', action='store_true', help='daemon server')
    parser.add_argument('--local', action='store_true', help='local server')
    parser.add_argument('--remote', action='store_true', help='remote server')
    parser.add_argument('src', type=str, default='', nargs='?', help='src path')
    parser.add_argument('dst', type=str, default='', nargs='?', help='dst path')
    parser.add_argument('-p', '--port', type=str, default='5555', help='if local, client port, else daemon port')
    parser.add_argument('--thread-num', type=int, default=3, help='sync thread num')
    parser.add_argument('--timeout', type=int, default=10, help='connect timeout second')
    parser.add_argument('--pipeline', type=int, default=10, help='file fetch pipeline')
    parser.add_argument('--chunksize', type=int, default=250000, help='chunksize for each pipeline')
    parser.add_argument('--exclude', type=str, action='append', help='exclude file or directory to sync')
    args = parser.parse_args()

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

    if not 1000 <= args.chunksize <= 500000:
        logging.error('chunksize is invalid, must be in [1000, 500000]')
        return False

    logging.debug(str(args))

    return args

# run in different mode
def run(args):
    if args.daemon:
        target = zsync_process.ZsyncDaemon(args.port)

    elif args.local:
        target = zsync_process.ZsyncLocalService(args.src, args.dst, args.port,
            args.pipeline, args.chunksize, args.thread_num,
            args.timeout, args.exclude)

    elif args.remote:
        target = zsync_process.ZsyncRemoteService(args.src, args.dst, args.port,
            args.pipeline, args.chunksize, args.thread_num,
            args.timeout, args.exclude)

    else:
        target = zsync_process.ZsyncClient(args.src, args.dst, args.port,
            args.pipeline, args.chunksize, args.thread_num,
            args.timeout, args.exclude)
    
    target.run()
    return

def main():
    prepare_log()
    args = prepare_args()
    if args:
        run(args)
    return

if __name__ == '__main__':
    main()
