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
import logging


class ZsyncThread(threading.Thread, Transceiver):
    def __init__(self, ctx, remote_port, remote_sock, 
            inproc_sock, timeout, pipeline, chunksize):

        threading.Thread.__init__(self)
        Transceiver.__init__(self)
        self.ctx = ctx
        self.timeout = timeout
        self.pipeline = pipeline
        self.chunksize = chunksize
        self.remote_port = remote_port
        self.remote_sock = remote_sock
        self.remote = Proxy(self, remote_sock)
        self.inproc_sock = inproc_sock
        self.identity = str(self.inproc_sock.getsockopt_string(zmq.IDENTITY))
        self.inproc = Proxy(self, inproc_sock)
        self.stoped = False
        self.file = zsync_utils.CommonFile()

        self.register()
        self.add_timeout(self.remote_sock, self.timeout)
        return

    def register(self):
        Transceiver.register(self, self.remote_sock)
        Transceiver.register(self, self.inproc_sock)
        return

    def do_stop(self, inproc):
        logging.debug('do_stop')
        self.stop()
        return

    def stop(self):
        self.stoped = True

    def log(self, msg, level=logging.DEBUG):
        self.inproc.on_child_log('thread %s: %s' % (self.identity, msg), level)
        return

    def run(self):
        self.log('%s runing' % self.__class__.__name__)
        while not self.stoped:
            polls = self.poll(1000)
            self.deal_poll(polls)

        return

class SendThread(ZsyncThread):
    def __init__(self, ctx, remote_port, remote_sock, 
            inproc_sock, timeout, pipeline, chunksize,
            src_path, file_queue):

        ZsyncThread.__init__(self, ctx, remote_port,
            remote_sock, inproc_sock, timeout, pipeline, chunksize)

        self.file_queue = file_queue
        self.src = zsync_utils.CommonPath(src_path)
        return

    def query_new_file(self, client):
        if not self.file_queue:
            client.send_over()
            self.stop()
            return

        file_path = self.file_queue.popleft()

        try:
            file_stat = os.stat(file_path)
        except Exception as e:
            client.do_stop(str(e))
            self.stop()
            return

        if os.path.isdir(file_path):
            file_type = config.FILE_TYPE_DIR
            file_size = 0
        else:
            file_type = config.FILE_TYPE_FILE
            file_size = file_stat.st_size

        file_mode = file_stat.st_mode
        file_time = file_stat.st_mtime

        client.on_new_file(os.path.relpath(file_path, self.src.prefix_path),
            file_type, file_mode, file_size, file_time)

        if file_type == config.FILE_TYPE_FILE:
            try:
                self.file.open(file_path, 'rb')
            except Exception as e:
                client.do_stop(str(e))
                self.stop()
                return
        return

    def fetch_file(self, client, offset):
        offset = int(offset)
        data = self.file.fetch(offset, self.chunksize)
        client.call_raw('on_fetch_file', str(offset), data)
        #self.log('send file offset %s len %s' % (offset, len(data)))
        return


class RecvThread(ZsyncThread):
    def __init__(self, ctx, remote_port, remote_sock, 
            inproc_sock, timeout, pipeline, chunksize, dst_path):

        ZsyncThread.__init__(self, ctx, remote_port,
            remote_sock, inproc_sock, timeout, pipeline, chunksize)

        self.dst = zsync_utils.CommonPath(dst_path)
        self.ready = True
        return

    def on_new_file(self, service, file_path, file_type, file_mode, file_size, file_mtime):
        file_path = os.path.join(self.dst.path, file_path)
        if file_type == config.FILE_TYPE_DIR:
            dir_name = file_path
            dir_mode = file_mode
            dir_time = file_mtime
        else:
            dir_name = os.path.dirname(file_path)
            dir_mode = None
            dir_time = file_mtime

            if zsync_utils.check_file_same(file_path, file_size, file_mtime):
                self.remote.query_new_file()
                return

        zsync_utils.fix_file_type(file_path, file_type)
        zsync_utils.fix_file_type(dir_name, config.FILE_TYPE_DIR)

        error = zsync_utils.makedir(dir_name, dir_mode)
        if error:
            self.log(error, logging.ERROR)
            self.remote.query_new_file()
            return

        if file_type == config.FILE_TYPE_DIR:
            self.remote.query_new_file()
            return

        try:
            self.file.open(file_path, 'wb', file_size, self.pipeline, file_mode, file_mtime)
        except Exception as e:
            service.do_stop(str(e))
            self.stop()
            return

        self.log('fetching file: %s size %s' % (file_path, file_size))
        self.sendfetch(service)
        return

    def sendfetch(self, service):
        while self.file.credit:
            if self.file.fetch_offset >= self.file.total:
                break
            
            service.call_raw('fetch_file', str(self.file.fetch_offset))
            self.file.fetch_offset += self.chunksize
            self.file.credit -= 1

        return

    def on_fetch_file(self, service, offset, data):
        # self.log('recv file offset %s len %s' % (offset, len(data)))
        self.file.write_chunk(int(offset), data)
        self.file.credit += 1
        if self.file.writedone:
            self.log('finish file %s' % self.file.path)
            service.query_new_file()
        else:
            self.sendfetch(service)
        return

    def send_over(self, service):
        self.stop()
        return

    def run(self):
        zsync_utils.fix_file_type(self.dst.path, config.FILE_TYPE_DIR)
        self.remote.query_new_file()

        ZsyncThread.run(self)
        return

class FileTransciver(Transceiver):
    def __init__(self, ctx, src_path, dst_path, 
            pipeline=0, chunksize=0, thread_num=0, 
            timeout=10, excludes=None):

        Transceiver.__init__(self)
        self.ctx = ctx
        self.src = zsync_utils.CommonPath(src_path)
        self.dst = zsync_utils.CommonPath(dst_path)
        self.remote_sock = None
        self.remote_ip = None
        self.remote_port = None
        self.remote = None
        self.sender = False
        self.pipeline = pipeline
        self.chunksize = chunksize
        self.thread_num = thread_num
        self.timeout = timeout
        self.excludes = None
        self.inproc_sock = None
        self.childs = []
        self.child_proxies = []
        self.file_queue = deque()
        self.stoped = False

        if excludes:
            self.excludes = zsync_utils.CommonExclude(excludes)
        return

    def on_child_log(self, child, msg, level):
        logging.log(level, msg)
        return

    def do_stop(self, remote, msg):
        logging.critical(msg)
        self.stop()
        return

    def stop(self):
        if self.stoped:
            return
        [child.do_stop() for child in self.child_proxies]
        [child.join() for child in self.childs]
        self.stoped = True
        return

    def has_child_alive(self):
        state = [child.is_alive() for child in self.childs]
        if any(state):
            return True
        return False

    def shake_hand(self, remote):
        self.del_timeout(remote.sock)
        remote.on_shake_hand()
        return

    def on_shake_hand(self, remote):
        self.del_timeout(remote.sock)
        return

    def remote_error(self, remote, msg):
        logging.critical(msg)
        self.stop()
        return

    @staticmethod
    def put_queue(self, dpath, fnames):
        if self.excludes:
            deln = set()
            for fname in fnames:
                if self.excludes.isExclude(os.path.relpath(dpath, self.src.prefix_path), fname):
                    deln.add(fname)
 
            fnames[:] = set(fnames) - deln

        self.file_queue.extend([os.path.join(dpath, fname) 
            for fname in fnames])
        return

    def prepare_sender(self):
        if not self.src.visitValid():
            logging.error('remote path not exist %s' % self.src.full())
            self.remote.remote_error('remote path not exist')
            return False

        if os.path.isdir(self.src.path):
            os.path.walk(self.src.path, self.put_queue, self)
        elif os.path.isfile(self.src.path):
            self.file_queue.append(self.src.path)
        else:
            logging.error('remote path is not dir nor file')
            self.remote.remote_error('remote path is not dir nor file')
            return False

        return True

    def set_remote_ports(self, service, ports):
        # logging.debug('set_remote_ports %s, %s' % (ports, self.sender))
        if len(ports) != self.thread_num:
            logging.critical('recv ports length is not equal to thread num: \
                thread_num=%s, ports=%s' % (self.thread_num, ports))
            return

        self.create_childs(ports)
        return

    def create_childs(self, ports=None):
        self.inproc, inproc_childs = zhelpers.zpipes(self.ctx, self.thread_num)
        child_identities = [str(inproc_child.getsockopt_string(zmq.IDENTITY)) \
            for inproc_child in inproc_childs]

        remote_socks = [zhelpers.nonblocking_socket(self.ctx, zmq.PAIR) \
            for i in xrange(self.thread_num)]

        if ports:
            for i, sock in enumerate(remote_socks):
                sock.connect('tcp://%s:%s' % (self.remote_ip, ports[i]))
            
        else:
            ports = []
            for i, sock in enumerate(remote_socks):
                port = zhelpers.bind_to_random_port(sock)
                ports.append(port)

            logging.debug('send remote ports')
            self.remote.set_remote_ports(ports)

        if self.sender:
            for i, sock in enumerate(remote_socks):
                self.childs.append(
                    SendThread(self.ctx, ports[i], sock, 
                        inproc_childs[i], self.timeout, self.pipeline,
                        self.chunksize, self.src.path, self.file_queue)
                    )
        else:
            for i, sock in enumerate(remote_socks):
                self.childs.append(
                    RecvThread(self.ctx, ports[i], sock,
                        inproc_childs[i], self.timeout, self.pipeline,
                        self.chunksize, self.dst.path)
                )

        self.child_proxies = [Proxy(self, self.inproc, child_identities[i]) \
            for i, inproc_child in enumerate(inproc_childs)]

        self.register(self.inproc)

        [child.start() for child in self.childs]
        return

    def _prepare(self):
        return False

    def run(self):
        if not self._prepare():
            logging.critical('prepare failed')
            return

        logging.debug('%s runing' % self.__class__.__name__)

        while not self.stoped:
            try:
                polls = self.poll(1000)
                self.deal_poll(polls)

            except KeyboardInterrupt:
                logging.info('user interrupted, exit')
                self.stop()
                if self.remote:
                    self.remote.do_stop('remote interrupt')

            if not self.check_timeout():
                logging.info('timeout, exit')
                self.stop()

            if self.childs and not self.has_child_alive():
                logging.info('all thread stop, exit')
                self.stop()

        return

