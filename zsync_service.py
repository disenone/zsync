# -*- coding: utf-8 -*-

import argparse
import os
from collections import deque
import threading
import cPickle
import time
import zmq
import zsync_thread

def run(args):
    ctx = zmq.Context()
    router = ctx.socket(zmq.ROUTER)

    router.connect('tcp://localhost:%s' % (args.p,))
    router.linger = 0
    poller = zmq.Poller()
    poller.register(router, zmq.POLLIN)

    task = None

    while True:
        try:
            socks = dict(poller.poll(1000))

            if socks.get(router) == zmq.POLLIN:
                msg = router.recv_multipart(zmq.NOBLOCK)
                print msg

                try:
                    id1, id2, path, thread_num = msg
                    thread_num = int(thread_num)
                except:
                    continue

                task = zsync_thread.ServiceManager(ctx, (id1, id2), router, path, thread_num)
                task.run()
                task = None

        except KeyboardInterrupt:
            print 'user interrupted, exit'
            break
    return

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', type=str, help='port', default='5556')

    args = parser.parse_args()
    run(args)
