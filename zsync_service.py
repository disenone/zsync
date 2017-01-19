# -*- coding: utf-8 -*-

import zmq
import argparse
import os
from collections import deque
import threading


class SendThread(threading.Thread):
    def __init__(self, ctx, thread_id, send_queue):
        self.ctx = ctx
        self.thread_id = thread_id
        self.send_queue = send_queue
        self.stoped = False
        self.ready = False

        # create pair socket to send file
        self.sock = ctx.socket(zmq.PAIR)
        self.sock.bind
        port = self.sock.bind_to_random_port('tcp://localhost', min_port=10000, max_port=11000, max_tries=1000)
        if not port:
            return
        self.port = port
        return

    def run(self):
        return

    def stop(self):
        self.stoped = True
        return

class NewSend(object):
    def __init__(self, id1, id2, router, path, thread_num):
        self.identity = (id1, id2)
        self.router = router
        self.path = path
        self.thread_num = thread_num
        self.send_queue = deque()
        return

    def send(self, *msgs):
        msgs = tuple(msgs)
        self.router.send_multipart(self.identity + msgs)
        return

    def error(self, msg):
        self.send('error', msg)
        return

    def run(self):
        if not os.path.exists(self.path):
            self.error('remote path not exist')
            return

        def put_queue(self, dpath, fname):
            self.send_queue.append(os.path.join(dpath, fname))
            return

        if os.path.isdir(self.path):
            os.path.walk(self.path, put_queue, self)
        elif os.path.isfile(self.path):
            self.send_queue.append(self.path)
        else:
            self.error('remote path is not dir nor file.')
            return

        return

def run(args):
    ctx = zmq.Context()
    router = ctx.socket(zmq.ROUTER)

    router.connect('tcp://%s:%s' % (args.ip, args.p))
    router.linger = 0
    poller = zmq.Poller()
    poller.register(router, zmq.POLLIN)

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

                task = NewSend(id1, id2, router, path, thread_num)
                task.run()

        except KeyboardInterrupt:
            print 'user interrupted, exit'
            break
    return

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-ip', type=str, help='broker ip', default='localhost')
    parser.add_argument('-p', type=str, help='port', default='5556')

    args = parser.parse_args()
    run(args)
