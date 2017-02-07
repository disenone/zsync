# -*- coding: utf-8 -*-

import threading
from collections import deque
import time
import zmq
import zhelpers
import zsync_utils
import os

class SendQueue(dict):
    def pushQueue(self, sock, msg):
        if sock not in self:
            self[sock] = deque()

        self[sock].append(msg)
        return

    def send(self, sock):
        if not self.get(sock):
            return

        queue = self[sock]
        msg = queue[0]
        try:
            sock.send_multipart(msg, zmq.NOBLOCK)
        except:
            return

        queue.popleft()
        return

class Transceiver(object):
    def __init__(self):
        self.send_queue = SendQueue()
        self.poller = zmq.Poller()
        return

    def send(self, sock, *msg):
        if not self.send_queue:
            try:
                sock.send_multipart(msg, zmq.NOBLOCK)
                return True
            except:
                self.send_queue.pushQueue(sock, msg)
        else:
            self.send_queue.pushQueue(sock, msg)

        return False

    def sendQueue(self, sock):
        self.send_queue.send(sock)
        return

    def addSend(self, sock, *msg):
        self.send_queue.pushQueue(sock, msg)
        return

    def poll(self, ms):
        return zhelpers.poll(self.poller, ms)

class ZsyncThread(threading.Thread, Transceiver):
    def __init__(self, ctx, timeout, pipeline=0, chunksize=0):
        threading.Thread.__init__(self)
        Transceiver.__init__(self)
        self.ctx = ctx
        self.timeout = timeout
        self.pipeline = pipeline
        self.chunksize = chunksize
        self.sock = None
        self.inproc = None
        self.stoped = False
        self.ready = False
        self.lastRecvTime = time.time()
        return

    def run(self):
        raise NotImplementedError

    def stop(self):
        self.stoped = True

    def recv(self):
        msg = self.sock.recv_multipart(zmq.NOBLOCK)
        self.lastRecvTime = time.time()
        return msg

    def register(self):
        self.poller.register(self.sock)
        self.poller.register(self.inproc)
        return


class SendThread(ZsyncThread):
    def __init__(self, ctx, thread_id, file_queue, timeout=10, pipeline=10, chunksize=250000):
        super(SendThread, self).__init__(ctx, timeout, pipeline, chunksize)
        self.thread_id = thread_id
        self.file_queue = file_queue

        # create pair socket to send file
        self.sock = ctx.socket(zmq.PAIR)
        port = self.sock.bind_to_random_port('tcp://*', min_port=10000,
                                             max_port=11000, max_tries=1000)
        if not port:
            return
        self.sock.linger = 0
        zhelpers.socket_set_hwm(self.sock, pipeline * 2)
        self.port = port
        self.register()

        self.ready = True
        return

    def cmd_init(self):
        self.addSend(str(self.pipeline), str(self.chunksize))

    def dispatch(self, msg):
        getattr(self, 'cmd_' + msg[0])(*msg[1:])
        return

    def run(self):
        if not self.ready:
            return

        while True:
            if self.stoped:
                break

            polls = self.poll(1000)

            if zmq.POLLIN in polls.get(self.sock, ()):
                msg = self.recv()
                self.dispatch(msg)
                print msg

            if zmq.POLLOUT in polls.get(self.sock, ()):
                self.sendQueue()

        print 'thread %d exit' % self.thread_id
        return


class RecvThread(ZsyncThread):
    def __init__(self, ctx, ip, port, timeout=10):
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
        self.addSend('init')

        while True:
            if self.stoped:
                break

            polls = self.poll(1000)

            if zmq.POLLIN in polls:
                msg = self.recv()
                print msg

            if zmq.POLLOUT in polls:
                self.sendQueue()

        return

class ThreadManager(Transceiver):
    def __init__(self, args):
        Transceiver.__init__(self)
        self.args = args
        self.ctx = zmq.Context()
        self.sock = None
        self.inproc = None
        self.inproc_childs = None
        self.childs = None
        return

    def cmd_log(self, msg):
        print msg
        return

    def register(self):
        self.poller.register(self.sock)
        self.poller.register(self.inproc)
        return


class WorkerManager(ThreadManager):
    def __init__(self, args):
        super(WorkerManager, self).__init__(args)
        return

    def parseArgs(self):
        self.src = zsync_utils.CommonFile(self.args.src)
        self.dst = zsync_utils.CommonFile(self.args.dst)

        if not self.src.isValid():
            print 'ERROR: src is invalid'
            return False

        if not self.dst.isValid():
            print 'ERROR: dst is invalid'
            return False

        if self.args.timeout <= 0:
            print 'ERROR: timeout is invalid, must be > 0'
            return False

        if self.args.thread_num <= 0:
            print 'ERROR: thread_num is invalid, must be > 0'
            return False

        self.sock = self.ctx.socket(zmq.DEALER)
        self.sock.connect('tcp://%s:%s' % (self.src.ip, self.args.p))
        self.sock.linger = 0
        self.inproc, self.inproc_childs = zhelpers.zpipes(self.ctx, self.args.thread_num, os.getpid())

        self.register()
        return True

    def run(self):
        if not self.parseArgs():
            return

        while True:
            polls = self.poll(1000)
            

        return

class ServiceManager(ThreadManager):
    def __init__(self, args):
        super(ServiceManager, self).__init__(args)
        return