# -*- coding: utf-8 -*-

import threading
from collections import deque
import time
import os
import cPickle
import binascii
import stat
import zmq
import zhelpers
import zsync_utils

FILE_TYPE_DIR = 'd'
FILE_TYPE_FILE = 'f'


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
        self.in_poller = zmq.Poller()
        self.all_poller = zmq.Poller()
        return

    def send(self, sock, *msg):
        if not msg:
            return
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

    def recv(self, sock):
        msg = sock.recv_multipart(zmq.NOBLOCK)
        return msg

    def poll(self, ms):
        if self.send_queue:
            return zhelpers.poll(self.all_poller, ms)
        else:
            return zhelpers.poll(self.in_poller, ms)

    def dispatch(self, sock, msg):
        #print 'dispatch', sock, msg
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
                    except Exception, e:
                        #print self.__class__.__name__, e
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
        self.file = zsync_utils.CommonFile()
        return

    def run(self):
        raise NotImplementedError

    def stop(self):
        self.stoped = True

    def register(self):
        self.in_poller.register(self.sock, zmq.POLLIN)
        self.in_poller.register(self.inproc, zmq.POLLIN)
        self.all_poller.register(self.sock)
        self.all_poller.register(self.inproc)
        self.lastRecvTime[self.sock] = self.lastRecvTime[self.inproc] = time.time()
        return

    def recv(self, sock):
        ret = Transceiver.recv(self, sock)
        self.lastRecvTime[sock] = time.time()
        return ret

    def poll(self, ms):
        ret = super(ZsyncThread, self).poll(ms)
        if zmq.POLLIN not in ret.get(self.sock, ()) and \
                time.time() - self.lastRecvTime[self.sock] > self.timeout:
            self.stop()
            self.log('thread %s recv timeout, exit' % self.identity)
        return ret

    def log(self, msg):
        self.send(self.inproc, 'log', 'thread %s: ' % self.identity + msg)
        return

class SendThread(ZsyncThread):
    def __init__(self, ctx, identity, file_queue, inproc, path, timeout=10, pipeline=10, chunksize=250000):
        super(SendThread, self).__init__(ctx, identity, inproc, timeout, pipeline, chunksize)
        self.file_queue = file_queue
        self.src = zsync_utils.CommonPath(path)

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

    def cmd_init(self):
        self.send(self.sock, 'init', str(self.pipeline), str(self.chunksize))
        return

    def cmd_stop(self):
        self.stop()
        return

    def cmd_newfile(self):
        if not self.file_queue:
            self.stop()
            self.send(self.sock, 'over')
            return
        file_path = self.file_queue.popleft()
        if os.path.isdir(file_path):
            file_type = FILE_TYPE_DIR
            file_size = '0'
        else:
            file_type = FILE_TYPE_FILE
            file_size = str(os.path.getsize(file_path))

        file_mode = str(os.stat(file_path)[stat.ST_MODE])

        self.send(self.sock, 'newfile', os.path.relpath(file_path, self.src.prefix_path),
            file_type, file_mode, file_size)

        self.file.open(file_path, 'rb')
        return

    def cmd_fetch(self, offset):
        offset = int(offset)
        data = self.file.fetch(offset, self.chunksize)
        self.send(self.sock, 'fetch', str(offset), data)
        #self.log('send file offset %s len %s' % (offset, len(data)))
        return

    def run(self):
        if not self.ready:
            return

        self.log('runing')

        while True:
            if self.stoped:
                break
            polls = self.poll(1000)
            self.deal_poll(polls)

        self.log('exit')
        return

class RecvThread(ZsyncThread):
    def __init__(self, ctx, identity, ip, port, inproc, path, timeout=10):
        super(RecvThread, self).__init__(ctx, identity, inproc, timeout)
        self.ip = ip
        self.port = port
        self.path = path

        # create pair socket to recv file
        self.sock = ctx.socket(zmq.PAIR)
        self.sock.connect('tcp://%s:%s' % (self.ip, self.port))
        self.sock.linger = 0
        self.register()

        self.ready = True
        return

    def askfile(self):
        self.send(self.sock, 'newfile')
        return

    def cmd_stop(self):
        self.stop()
        return

    def cmd_init(self, pipeline, chunksize):
        self.pipeline = int(pipeline)
        self.chunksize = int(chunksize)
        self.askfile()
        return

    def cmd_newfile(self, file_path, file_type, file_mode, file_size):
        file_mode = int(file_mode)
        file_size = int(file_size)
        file_path = os.path.join(self.path, file_path)
        if file_type == FILE_TYPE_DIR:
            dir_name = file_path
            dir_mode = file_mode
        else:
            dir_name = os.path.dirname(file_path)
            dir_mode = None

        error = zsync_utils.makedir(dir_name, dir_mode)
        if error:
            self.log('ERROR: ' + error)
            self.askfile()
            return

        if file_type == FILE_TYPE_DIR:
            self.askfile()
            return

        self.file.open(file_path, 'wb', file_size, self.pipeline)
        self.log('fetching file: %s size %s' % (file_path, file_size))
        self.sendfetch()
        return

    def sendfetch(self):
        while self.file.credit:
            if self.file.fetch_offset >= self.file.total:
                break
            self.send(self.sock, 'fetch',
                str(self.file.fetch_offset))

            self.file.fetch_offset += self.chunksize
            self.file.credit -= 1

        return

    def cmd_fetch(self, offset, data):
        #self.log('recv file offset %s len %s' % (offset, len(data)))
        self.file.write_chunk(int(offset), data)
        self.file.credit += 1
        if self.file.writedone:
            self.log('finish file %s' % self.path)
            self.askfile()
        else:
            self.sendfetch()
        return

    def cmd_over(self):
        self.stop()
        self.log('file sync over, exit')
        return

    def run(self):
        self.send(self.sock, 'init')
        self.log('thread %s runing' % self.identity)

        while not self.stoped:
            polls = self.poll(1000)
            self.deal_poll(polls)

        self.log('thread %s exit' % self.identity)
        return

class ChildThread(object):
    def __init__(self, inproc, identity, thread=None):
        self.inproc = inproc
        self.identity = identity
        zhelpers.set_id(self.inproc, unicode(identity))
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
        self.in_poller.register(self.sock, zmq.POLLIN)
        self.in_poller.register(self.inproc, zmq.POLLIN)
        self.all_poller.register(self.sock)
        self.all_poller.register(self.inproc)
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
        self.src = zsync_utils.CommonPath(self.args.src)
        self.dst = zsync_utils.CommonPath(self.args.dst)

        if not self.src.isValid():
            print 'ERROR: src is invalid'
            return False

        if not self.dst.isValid():
            print 'ERROR: dst is invalid'
            return False

        if not 0 < self.args.timeout <= 300:
            print 'ERROR: timeout is invalid, must be in [1, 300]'
            return False

        if self.args.thread_num <= 0:
            print 'ERROR: thread_num is invalid, must be > 0'
            return False

        self.sock = self.ctx.socket(zmq.DEALER)
        self.sock.connect('tcp://%s:%s' % (self.src.ip, self.args.p))
        self.sock.linger = 0

        addr = str(os.getpid()) + '-' + binascii.hexlify(os.urandom(8))
        self.inproc, inproc_childs = zhelpers.zpipes(self.ctx, self.args.thread_num, addr)
        self.childs = [ChildThread(inproc, str(i)) for inproc, i in 
            zip(inproc_childs, range(len(inproc_childs)))]

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
            child.thread = RecvThread(self.ctx, child.identity, 
                self.src.ip, port, child.inproc, self.dst.path, timeout=self.args.timeout)
      
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

        self.send(self.sock, self.src.path, str(self.args.thread_num),
            str(self.args.timeout), str(cPickle.dumps(self.args.exclude)))

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
    def __init__(self, ctx, identity, router, path, thread_num, timeout, excludes):
        super(ServiceManager, self).__init__(ctx)
        self.identity = tuple(identity)
        self.sock = router
        self.path = path
        self.thread_num = thread_num
        self.timeout = timeout
        self.excludes = excludes
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
            # identity = tuple(msg[-2:])
            # if identity != 

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

        self.childs = [ChildThread(inproc, str(i)) for inproc, i in 
            zip(inproc_childs, range(len(inproc_childs)))]

        self.register()
        return True

    def run(self):
        if not self.parse_path():
            return

        for child in self.childs:
            child.thread = SendThread(self.ctx, child.identity, 
                self.file_queue, child.inproc, self.path, timeout=self.timeout)

        thread_ports = [child.thread.port for child in self.childs]

        self.send(self.sock, 'port', cPickle.dumps(thread_ports))
        [child.thread.start() for child in self.childs]

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
