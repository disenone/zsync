# -*- coding: utf-8 -*-

import sys
import argparse
import zsync_utils
import threading
import time
import zmq
import zsync_thread


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('src', type=str, help='src', default='')
    parser.add_argument('dst', type=str, help='dst', default='')
    parser.add_argument('-p', type=str, help='port', default='5555')
    parser.add_argument('--thread-num', type=int, default=3, help='sync thread num')
    parser.add_argument('--exclude', type=str, action='append', help='exclude file or directory to sync')
    parser.add_argument('--timeout', type=int, default=5, help='connect timeout second')

    args = parser.parse_args()

    print args
    # parser.print_help()
    tbegin = time.time()
    worker = zsync_thread.WorkerManager(zmq.Context(), args)
    worker.run()
    tend = time.time()
    print 'cost time: %s' % (tend - tbegin)
