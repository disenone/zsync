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
    logging.debug('program begin.')
    return

def prepare_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--daemon', action='store_true', help='daemon server')
    parser.add_argument('--local', action='store_true', help='local server')
    parser.add_argument('--remote', action='store_true', help='remote server')
    parser.add_argument('--sender', action='store_true', help='server is sender')
    parser.add_argument('src', type=str, default='', nargs='?', help='src path')
    parser.add_argument('dst', type=str, default='', nargs='?', help='dst path')
    parser.add_argument('-p', '--port', type=str, default='5555', help='if local, client port, else daemon port')
    parser.add_argument('--thread-num', type=int, default=3, help='sync thread num')
    parser.add_argument('--exclude', type=str, action='append', help='exclude file or directory to sync')
    parser.add_argument('--timeout', type=int, default=5, help='connect timeout second')
    args = parser.parse_args()
    return args

# run in different mode
def run(args):
    if args.daemon:
        target = zsync_process.ZsyncDaemon(args.port)

    elif args.local:
        target = zsync_process.ZsyncLocalService(args.port, args.timeout)

    elif args.remote:
        target = zsync_process.ZsyncRemoteService(args.port, args.timeout)

    else:
        target = zsync_process.ZsyncClient(args.src, args.dst, args.port,
            args.thread_num, args.timeout, args.exclude)
    
    target.run()
    return

def main():
    prepare_log()
    args = prepare_args()
    run(args)
    return

if __name__ == '__main__':
    main()
