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
from zsync_network import Transceiver, Proxy
import config


class ZsyncThread(threading.Thread, Transceiver):
    def __init__(self, ctx, identity, remote_sock, inproc_sock, timeout, pipeline=0, chunksize=0):
        threading.Thread.__init__(self)
        Transceiver.__init__(self)
        self.ctx = ctx
        self.identity = identity
        self.timeout = timeout
        self.pipeline = pipeline
        self.chunksize = chunksize
        self.remote_sock = None
        self.remote_port= None
        self.remote = None
        self.inproc_sock = inproc_sock
        self.inproc = Proxy(self, inproc_sock)
        self.stoped = False
        self.ready = False
        self.file = zsync_utils.CommonFile()

        self.register()
        self.add_timeout(self.remote_sock, self.timeout)
        return

    def register(self):
        Transceiver.register(self, self.remote_sock)
        Transceiver.register(self, self.inproc_sock)
        return

    def run(self):
        raise NotImplementedError

    def stop(self):
        self.stoped = True

    def log(self, msg):
        self.parent.on_child_log('thread %s: %s' % (self.identity, msg))
        return

class SendThread(ZsyncThread):
    def __init__(self, ctx, identity, file_queue, inproc, path, timeout=10, pipeline=10, chunksize=250000):
        ZsyncThread.__init__(self, ctx, identity, inproc, timeout, pipeline, chunksize)
        self.file_queue = file_queue
        self.src = zsync_utils.CommonPath(path)

        # create pair socket to send file
        self.remote_sock = zhelpers.nonblocking_socket(self.ctx, zmq.PAIR)
        port = zhelpers.bind_to_random_port(self.remote_sock)
        if not port:
            return
        self.remote_port = port
        self.add_timeout(self.remote_sock, self.timeout)

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
            file_type = config.FILE_TYPE_DIR
            file_size = '0'
        else:
            file_type = config.FILE_TYPE_FILE
            file_size = str(os.path.getsize(file_path))

        file_mode = str(os.stat(file_path)[stat.ST_MODE])

        self.send(self.sock, 'newfile', os.path.relpath(file_path, self.src.prefix_path),
            file_type, file_mode, file_size)

        if file_type == config.FILE_TYPE_FILE:
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

        self.log('begin')

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
        self.path = zsync_utils.CommonPath(path)
        self.thread_num = thread_num
        self.timeout = timeout
        self.excludes = None
        if excludes:
            self.excludes = zsync_utils.CommonExclude(excludes)
        self.file_queue = deque()
        return

    @staticmethod
    def put_queue(self, dpath, fnames):
        if self.excludes:
            deln = set()
            for fname in fnames:
                if self.excludes.isExclude(os.path.relpath(dpath, self.path.prefix_path), fname):
                    deln.add(fname)
 
            fnames[:] = set(fnames) - deln

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
        if not os.path.exists(self.path.path):
            self.send(self.sock, 'error', 'remote path not exist')
            return False

        if os.path.isdir(self.path.path):
            os.path.walk(self.path.path, self.put_queue, self)
        elif os.path.isfile(self.path.path):
            self.file_queue.append(self.path.path)
        else:
            self.send(self.sock, 'error', 'remote path is not dir nor file')
            return False

        addr = str(os.getpid()) + '-' + binascii.hexlify(os.urandom(8))
        self.inproc, inproc_childs = zhelpers.zpipes(self.ctx, self.thread_num, addr)

        self.childs = [ChildThread(inproc, str(i)) for inproc, i in \
            zip(inproc_childs, range(len(inproc_childs)))]

        self.register()
        return True

    def run(self):
        if not self.parse_path():
            return

        for child in self.childs:
            child.thread = SendThread(self.ctx, child.identity, 
                self.file_queue, child.inproc, self.path.path, timeout=self.timeout)

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
