# -*- coding: utf-8 -*-

import zmq
import argparse
import os
from collections import deque
import threading
import cPickle
import time


class SendThread(threading.Thread):
    def __init__(self, ctx, thread_id, send_queue):
        threading.Thread.__init__(self)  
        self.ctx = ctx
        self.thread_id = thread_id
        self.send_queue = send_queue
        self.stoped = False
        self.ready = False

        # create pair socket to send file
        self.sock = ctx.socket(zmq.PAIR)
        port = self.sock.bind_to_random_port('tcp://*', min_port=10000, max_port=11000, max_tries=1000)
        if not port:
            return
        self.sock.linger = 0
        self.port = port
        self.ready = True
        return

    def run(self):
        if not self.ready:
            return

        poller = zmq.Poller()
        poller.register(self.sock, zmq.POLLIN)

        while True:
            print 'thread %d, ' % self.thread_id,  self.stoped
            if self.stoped:
                break

            print 'go in poll'
            socks = dict(poller.poll(1000))
            if socks.get(self.sock) == zmq.POLLIN:
                msg = self.sock.recv_multipart(zmq.NOBLOCK)
                print msg
            print 'go out poll'

            print 'before send'
            self.send('test', 'hello world!')
            print 'sending msg'
        
        print 'thread %d exit' % self.thread_id

        return

    def stop(self):
        self.stoped = True
        return

    def send(self, *msgs):
        self.sock.send_multipart(msgs)
        return

class NewSend(object):
    def __init__(self, id1, id2, ctx, router, path, thread_num):
        self.ctx = ctx
        self.identity = (id1, id2)
        self.router = router
        self.path = path
        self.thread_num = thread_num
        self.send_queue = deque()
        self.threads = []
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

        def put_queue(self, dpath, fnames):
            self.send_queue.extend([os.path.join(dpath, fname) for fname in fnames])
            return

        if os.path.isdir(self.path):
            os.path.walk(self.path, put_queue, self)
        elif os.path.isfile(self.path):
            self.send_queue.append(self.path)
        else:
            self.error('remote path is not dir nor file')
            return

        self.threads = [SendThread(self.ctx, i, self.send_queue) for i in xrange(self.thread_num)]
        if any([not thread.ready for thread in self.threads]):
            self.error('remote thread init failed')
            return

        thread_ports = [thread.port for thread in self.threads]

        self.send('port', cPickle.dumps(thread_ports))

        [thread.start() for thread in self.threads]

        #self.send('test', 'there')

        while True:
            try:
                time.sleep(1)
                states = [thread.stoped for thread in self.threads]
                if all(states):
                    break
                print 'checking states', states
            except KeyboardInterrupt:
                [thread.stop() for thread in self.threads]
                raise
        return

    def stop(self):
        [thread.stop() for thread in self.threads]
        return

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

                task = NewSend(id1, id2, ctx, router, path, thread_num)
                task.run()

        except KeyboardInterrupt:
            if task:
                task.stop()
            print 'user interrupted, exit'
            break
    return

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', type=str, help='port', default='5556')

    args = parser.parse_args()
    run(args)
    print 'exit here'