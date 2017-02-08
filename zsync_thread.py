# -*- coding: utf-8 -*-

import threading
from collections import deque
import time
import zmq
import zhelpers
import zsync_utils
import os
import cPickle
import binascii


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
                print 'send: ', msg
                return True
            except:
                self.send_queue.pushQueue(sock, msg)
        else:
            self.send_queue.pushQueue(sock, msg)

        return False

    def sendQueue(self, sock):
        self.send_queue.send(sock)
        return

    def recv(self, sock):
        msg = sock.recv_multipart(zmq.NOBLOCK)
        print 'recv: ', msg
        return msg

    def poll(self, ms):
        return zhelpers.poll(self.poller, ms)

    def dispatch(self, sock, msg):
        print 'dispatch', sock, msg
        cmd = msg[0]
        funcn = 'cmd_' + str(cmd)

        func = getattr(self, funcn, None)
        if not func:
            print 'ERROR: invalid command "%s"' % cmd
            return
        
        return func(*msg[1:])

    def deal_poll(self, polls):
        if not polls:
            return

        for sock, state in polls.iteritems():
            if zmq.POLLIN in state:
                while True:
                    try:
                        msg = self.recv(sock)
                    except:
                        break
                    self.dispatch(sock, msg)

            if zmq.POLLOUT in state:
                self.sendQueue(sock)
        return

class ZsyncThread(threading.Thread, Transceiver):
    def __init__(self, ctx, identity, inproc, timeout, pipeline=0, chunksize=0):
        threading.Thread.__init__(self)
        Transceiver.__init__(self)
        self.ctx = ctx
        self.identity = identity
        self.timeout = timeout
        self.pipeline = pipeline
        self.chunksize = chunksize
        self.sock = None
        self.inproc = inproc
        self.stoped = False
        self.ready = False
        self.lastRecvTime = {}
        return

    def run(self):
        raise NotImplementedError

    def stop(self):
        self.stoped = True

    def register(self):
        self.poller.register(self.sock)
        self.poller.register(self.inproc)
        self.lastRecvTime[self.sock] = self.lastRecvTime[self.inproc] = time.time()
        return

    def recv(self, sock):
        ret = Transceiver.recv(self, sock)
        self.lastRecvTime[sock] = time.time()

    def poll(self, ms):
        ret = super(ZsyncThread, self).poll(ms)
        if zmq.POLLIN not in ret.get(self.sock, ()) and time.time() - self.lastRecvTime[self.sock] > self.timeout:
            self.stop()
            self.log('recv timeout, exit')

        return

    def log(self, msg):
        self.send(self.inproc, 'log', msg)
        return

class SendThread(ZsyncThread):
    def __init__(self, ctx, identity, file_queue, inproc, timeout=10, pipeline=10, chunksize=250000):
        super(SendThread, self).__init__(ctx, identity, inproc, timeout, pipeline, chunksize)
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
        self.send(self.sock, str(self.pipeline), str(self.chunksize))
        return

    def cmd_stop(self):
        self.stop()
        return

    def run(self):
        if not self.ready:
            return

        self.log('thread %s runing' % self.identity)

        while True:
            if self.stoped:
                break
            polls = self.poll(1000)
            self.deal_poll(polls)

            #self.send(self.sock, 'test', 'hello')

        self.log('thread %s exit' % self.identity)
        time.sleep(1)
        return


class RecvThread(ZsyncThread):
    def __init__(self, ctx, identity, ip, port, inproc, timeout=10):
        super(RecvThread, self).__init__(ctx, identity, inproc, timeout)
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
        self.send('init')
        #self.log('thread %s runing' % self.identity)

        while not self.stoped:

            polls = self.poll(1000)
            self.deal_poll(polls)

        self.log('thread %s exit' % self.identity)
        time.sleep(1)
        return

class ChildThread(object):
    def __init__(self, inproc, identity, thread=None):
        self.inproc = inproc
        self.identity = identity
        zhelpers.set_id(self.inproc, identity)
        self.thread = thread
        return


class ThreadManager(Transceiver):
    def __init__(self, ctx):
        Transceiver.__init__(self)
        self.ctx = ctx
        self.sock = None
        self.inproc = None
        self.childs = []
        self.stoped = False
        return

    def cmd_log(self, msg, *identity):
        print msg
        return

    def register(self):
        self.poller.register(self.sock)
        self.poller.register(self.inproc)
        return

    def stop(self):
        if self.stoped:
            return
        self.send(self.inproc, 'stop')
        [child.thread.join() for child in self.childs if child.thread]
        self.stoped = True
        return

    def has_child_alive(self):
        state = [child.thread.is_alive() for child in self.childs if child.thread]
        if any(state):
            return True
        return False

    def send(self, sock, *msg):
        if sock == self.inproc:
            self.broadcast_child(*msg)
        else:
            Transceiver.send(self, sock, *msg)
        return

    def broadcast_child(self, *msg):
        for child in self.childs:
            self.send_child(child.identity, *msg)
        return

    def send_child(self, identity, *msg):
        Transceiver.send(self, self.inproc, identity, *msg)
        return

class WorkerManager(ThreadManager):
    def __init__(self, ctx, args):
        super(WorkerManager, self).__init__(ctx)
        self.args = args
        self.started = False
        return

    def parse_args(self):
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

        addr = str(os.getpid()) + '-' + binascii.hexlify(os.urandom(8))
        self.inproc, inproc_childs = zhelpers.zpipes(self.ctx, self.args.thread_num, addr)
        self.childs = [ChildThread(inproc, unicode(i)) for inproc, i in zip(inproc_childs, range(len(inproc_childs)))]

        self.register()
        
        # 10秒的连接时间，如果超过10秒都没有收到一次数据，判断连接不上
        self.connected = False
        self.connect_time = time.time()
        return True

    def dispatch(self, sock, msg):
        if sock == self.sock and not self.connected:
            self.connected = True

        if sock == self.inproc:
            msg = msg[1:] + [msg[0]]

        return Transceiver.dispatch(self, sock, msg)

    def cmd_error(self, msg):
        print 'ERROR: %s' % msg
        self.stop()
        return

    def cmd_port(self, ports):
        ports = cPickle.loads(ports)

        for i, child in enumerate(self.childs):
            port = ports[i]
            child.thread = RecvThread(self.ctx, child.identity, self.src.ip, port, child.inproc)
      
        [child.thread.start() for child in self.childs]
        self.started = True
        return

    def check_connect(self):
        if not self.connected and time.time() - self.connect_time > self.args.timeout:
            print 'connect timeout, exit'
            self.stop()
        return

    def run(self):
        if not self.parse_args():
            return

        self.send(self.sock, self.src.path, str(self.args.thread_num))

        while not self.stoped:
            try:
                polls = self.poll(1000)
                self.deal_poll(polls)
                self.check_connect()

            except KeyboardInterrupt:
                print 'user interrupted, exit'
                self.stop()

            if self.started and not self.has_child_alive():
                print 'all thread stop, exit'
                self.stop()
        return

class ServiceManager(ThreadManager):
    def __init__(self, ctx, identity, router, path, thread_num):
        super(ServiceManager, self).__init__(ctx)
        self.identity = tuple(identity)
        self.sock = router
        self.path = path
        self.thread_num = thread_num
        self.file_queue = deque()
        return

    @staticmethod
    def put_queue(self, dpath, fnames):
        self.file_queue.extend([os.path.join(dpath, fname) 
            for fname in fnames])
        return

    def send(self, sock, *msg):
        if sock == self.inproc:
            self.broadcast_child(*msg)
        elif sock == self.sock:
            msg = self.identity + msg
            Transceiver.send(self, sock, *msg)
        else:
            Transceiver.send(self, sock, *msg)
        return

    def dispatch(self, sock, msg):
        if sock == self.inproc:
            msg = msg[1:] + [msg[0]]

        if sock == self.sock:
            msg = msg[2:] + msg[:2]

        return Transceiver.dispatch(self, sock, msg)

    def parse_path(self):
        if not os.path.exists(self.path):
            self.send(self.sock, 'error', 'remote path not exist')
            return False

        if os.path.isdir(self.path):
            os.path.walk(self.path, self.put_queue, self)
        elif os.path.isfile(self.path):
            self.file_queue.append(self.path)
        else:
            self.send(self.sock, 'error', 'remote path is not dir nor file')
            return False

        addr = str(os.getpid()) + '-' + binascii.hexlify(os.urandom(8))
        self.inproc, inproc_childs = zhelpers.zpipes(self.ctx, self.thread_num, addr)

        self.childs = [ChildThread(inproc, unicode(i)) for inproc, i in zip(inproc_childs, range(len(inproc_childs)))]

        self.register()
        return True

    def run(self):
        if not self.parse_path():
            return

        for child in self.childs:
            child.thread = SendThread(self.ctx, child.identity, self.file_queue, child.inproc)

        thread_ports = [child.thread.port for child in self.childs]

        self.send(self.sock, 'port', cPickle.dumps(thread_ports))
        [child.thread.start() for child in self.childs]

        print 'service thread started'
        while not self.stoped:
            try:
                polls = self.poll(1000)
                self.deal_poll(polls)

            except KeyboardInterrupt:
                print 'user interrupted, exit'
                self.stop()

            if not self.has_child_alive():
                print 'all thread stop, exit'
                self.stop()
        return
