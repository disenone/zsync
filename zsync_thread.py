# -*- coding: utf-8 -*-

import threading
from collections import deque
import time
import zmq


class ZsyncThread(threading.Thread):
    def __init__(self, ctx, timeout):
        threading.Thread.__init__(self)
        self.ctx = ctx
        self.timeout = timeout
        self.sock = None
        self.stoped = False
        self.ready = False
        self.send_queue = deque()
        self.poller = zmq.Poller()
        self.lastRecvTime = time.time()
        return

    def run(self):
        raise NotImplementedError

    def stop(self):
        self.stoped = True

    def send(self, msg):
        try:
            self.sock.send_multipart(msg, zmq.NOBLOCK)
            return True
        except:
            self.send_queue.append(msg)
        return False

    def sendQueue(self):
        if not self.send_queue:
            return

        while self.send_queue:
            msg = self.send_queue[0]
            if not self.send(msg):
                break
            self.send_queue.popleft()
        return

    def recv(self):
        msg = self.sock.recv_multipart(zmq.NOBLOCK)
        self.lastRecvTime = time.time()
        return msg

    def register(self):
        self.poller.register(self.sock)

    def poll(self, ms):
        if time.time() - self.lastRecvTime > self.timeout:
            self.stop()
            return []

        socks = dict(self.poller.poll(ms))
        states = []
        if self.sock not in socks:
            return []

        state = socks[self.sock]
        if state & zmq.POLLIN == zmq.POLLIN:
            states.append(zmq.POLLIN)
        if state & zmq.POLLOUT == zmq.POLLOUT:
            states.append(zmq.POLLOUT)
        return states


class SendThread(ZsyncThread):
    def __init__(self, ctx, thread_id, file_queue, timeout):
        super(SendThread, self).__init__(ctx, timeout)
        self.thread_id = thread_id
        self.file_queue = file_queue

        # create pair socket to send file
        self.sock = ctx.socket(zmq.PAIR)
        port = self.sock.bind_to_random_port('tcp://*', min_port=10000,
                                             max_port=11000, max_tries=1000)
        if not port:
            return
        self.sock.linger = 0
        self.port = port
        self.register()

        self.ready = True
        return

    def run(self):
        if not self.ready:
            return

        while True:
            if self.stoped:
                break

            polls = self.poll(1000)
            if zmq.POLLIN in polls:
                msg = self.recv()
                print msg

            if zmq.POLLOUT in polls:
                self.send(('test', 'hello world!'))

        print 'thread %d exit' % self.thread_id
        return


class RecvThread(ZsyncThread):
    def __init__(self, ctx, ip, port, timeout):
        super(RecvThread, self).__init__(ctx, timeout)
        self.ip = ip
        self.port = port

        # create pair socket to recv file
        self.sock = ctx.socket(zmq.PAIR)
        self.sock.connect('tcp://%s:%s' % (self.ip, self.port))
        self.sock.linger = 0
        self.register()

        self.ready = True
        return

    def run(self):
        while True:
            if self.stoped:
                break

            polls = self.poll(1000)

            if zmq.POLLIN in polls:
                msg = self.recv()
                self.send_queue.append(msg)

            if zmq.POLLOUT in polls:
                self.sendQueue()

        return
