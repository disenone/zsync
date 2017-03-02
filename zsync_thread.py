# -*- coding: utf-8 -*-

import threading
from collections import deque
import os
import zmq
import zhelpers
import zsync_utils
from zsync_network import Transceiver, Proxy
import config
import logging
import zlib
from zsync_logger import MYLOGGER, log_file_progress


class ZsyncThread(threading.Thread, Transceiver):
    def __init__(self, ctx, remote_port, remote_sock, 
            inproc_sock, timeout, pipeline, chunksize, compress):

        threading.Thread.__init__(self)
        Transceiver.__init__(self)
        self.ctx = ctx
        self.timeout = timeout
        self.pipeline = pipeline
        self.chunksize = chunksize
        self.compress = compress
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

    def do_stop(self, inproc, msg=''):
        if msg:
            MYLOGGER.debug(msg)
        self.stop()
        return

    def stop(self):
        self.stoped = True

    def log(self, msg, level=logging.INFO):
        self.inproc.on_child_log('thread %s: %s' % (self.identity, msg), level)
        return

    def run(self):
        self.log('%s runing' % self.__class__.__name__)
        while not self.stoped:
            polls = self.poll(1000)
            self.deal_poll(polls)

            if not self.check_timeout():
                if self.file.is_open():
                    self.sync_file_timeout()
                    self.delay_all_timeout()
                else:
                    self.stop()
        return

    def remote_msg(self, remote, msg, level=logging.DEBUG):
        self.log('remote: ' + msg, level)
        return

    def sync_file_timeout(self):
        self.file.close()
        return

class SendThread(ZsyncThread):
    def __init__(self, ctx, remote_port, remote_sock, 
            inproc_sock, timeout, pipeline, chunksize,
            src_path, file_queue, compress):

        ZsyncThread.__init__(self, ctx, remote_port,
            remote_sock, inproc_sock, timeout, pipeline, chunksize, compress)

        self.file_queue = file_queue
        self.src = zsync_utils.CommonPath(src_path)
        return

    def try_send_new_file(self, client):
        self.file.close()
        if not self.file_queue:
            client.send_over()
            self.stop()
            return True

        file_path = self.file_queue.popleft()

        if os.path.islink(file_path):
            client.remote_msg('skip link file: %s' % file_path)
            return False

        try:
            file_stat = os.stat(file_path)
            file_type = config.FILE_TYPE_DIR if os.path.isdir(file_path) \
                else config.FILE_TYPE_FILE
            if file_type == config.FILE_TYPE_FILE:
                self.file.open(file_path, 'rb')
        except Exception as e:
            self.log(str(e), logging.ERROR)
            client.remote_msg(str(e), logging.ERROR)
            return False

        file_size = file_stat.st_size
        file_mode = file_stat.st_mode
        file_time = file_stat.st_mtime
        self.file.total = file_size

        client.on_new_file(os.path.relpath(file_path, self.src.prefix_path),
            file_type, file_mode, file_size, file_time)

        return True

    def query_new_file(self, client):
        while not self.try_send_new_file(client):
            pass

        return

    def fetch_file(self, client, offset):
        offset = int(offset)
        data = self.file.fetch(offset, self.chunksize)

        if self.compress:
            data = zlib.compress(data)

        client.call_raw('on_fetch_file', str(offset), data)
        #self.log('send file offset %s len %s' % (offset, len(data)))
        return

    def retry_file(self, client, file_path):
        file_path = os.path.join(self.src.prefix_path, file_path)
        self.log('sync file failed, auto put in queue again and retry: %s' % \
            file_path, logging.ERROR)
        self.file_queue.append(file_path)
        self.query_new_file(client)
        return


class RecvThread(ZsyncThread):
    def __init__(self, ctx, remote_port, remote_sock, 
            inproc_sock, timeout, pipeline, chunksize, dst_path, compress):

        ZsyncThread.__init__(self, ctx, remote_port,
            remote_sock, inproc_sock, timeout, pipeline, chunksize, compress)

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
            dir_time = None

            if zsync_utils.check_file_same(file_path, file_size, file_mtime):
                service.query_new_file()
                return

        zsync_utils.fix_file_type(file_path, file_type)
        zsync_utils.fix_file_type(dir_name, config.FILE_TYPE_DIR)

        error = zsync_utils.makedir(dir_name, dir_mode, dir_time)
        if error:
            self.log(error, logging.ERROR)
            service.query_new_file()
            return

        if file_type == config.FILE_TYPE_DIR:
            service.query_new_file()
            return

        try:
            self.file.open(file_path, 'wb', file_size, self.pipeline, file_mode, file_mtime)
        except Exception as e:
            service.query_new_file()
            self.log(str(e), logging.ERROR)
            return

        if file_size == 0:
            self.file.close()
            service.query_new_file()
            return

        #self.log('fetching file: %s size %s' % (file_path, file_size))
        self.sendfetch(service)
        return

    def sendfetch(self, service):
        while self.file.credit:
            if self.file.fetch_offset >= self.file.total:
                break
            
            # waiting for pre chunk
            if len(self.file.chunk_map) > 10:
                break

            service.call_raw('fetch_file', str(self.file.fetch_offset))
            self.file.fetch_offset += self.chunksize
            self.file.credit -= 1

        return

    def on_fetch_file(self, service, offset, data):
        # self.log('recv file offset %s len %s' % (offset, len(data)))
        if self.compress:
            data = zlib.decompress(data)
        self.file.write_chunk(int(offset), data)
        self.file.credit += 1
        if self.file.writedone:
            self.log('finish file %s' % self.file.path)
            service.query_new_file()
        else:
            self.sendfetch(service)
        return

    def sync_file_timeout(self):
        self.file.close()
        relpath = os.path.relpath(self.file.path, self.dst.path)
        self.remote.retry_file(relpath)
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
    def __init__(self, ctx, args):

        Transceiver.__init__(self)
        self.args = args
        self.ctx = ctx
        self.remote_sock = None
        self.remote_ip = None
        self.remote_port = None
        self.remote = None
        self.sender = False
        self.inproc_sock = None
        self.childs = []
        self.child_proxies = []
        self.file_queue = deque()
        self.stoped = False
        self.sub = None

        self.src = zsync_utils.CommonPath(self.args.src)
        self.dst = zsync_utils.CommonPath(self.args.dst)
        self.excludes = zsync_utils.CommonExclude(self.args.excludes)
        return

    def remote_msg(self, remote, msg, level=logging.DEBUG):
        MYLOGGER.log(level, 'remote: ' + msg)
        return

    def log(self, level, msg):
        if level >= logging.ERROR and self.remote:
            self.remote.remote_msg(msg, level)
        MYLOGGER.log(level, msg)
        return

    def on_child_log(self, child, msg, level):
        self.log(level, msg)
        return

    def do_stop(self, remote, msg='', level=logging.DEBUG):
        if msg:
            self.log(level, msg)
        self.stop()
        return

    def stop(self):
        if self.stoped:
            return
        [child.do_stop() for child in self.child_proxies]
        [child.join() for child in self.childs]
        if self.sub:
            self.sub.wait()
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
            self.log(logging.ERROR, 'path not exist %s' % self.src.origin_path)
            self.remote.do_stop()
            return False

        if os.path.islink(self.src.path):
            self.log(logging.ERROR, 'path is not supported link %s' % self.src.origin_path)
            self.remote.do_stop()
            return False
        if os.path.isdir(self.src.path):
            os.path.walk(self.src.path, self.put_queue, self)
        elif os.path.isfile(self.src.path):
            self.file_queue.append(self.src.path)
        else:
            self.log(logging.ERROR, 'path is not dir nor file %s' % self.src.origin_path)
            self.remote.do_stop()
            return False

        return True

    def set_remote_ports(self, service, ports):
        # logging.debug('set_remote_ports %s, %s' % (ports, self.sender))
        if len(ports) != self.args.thread_num:
            self.log(logging.CRITICAL, 'recv ports length is not equal to thread num: \
                thread_num=%s, ports=%s' % (self.args.thread_num, ports))
            service.do_stop()
            return

        self.create_childs(ports)
        return

    def create_childs(self, ports=None):
        self.inproc, inproc_childs = zhelpers.zpipes(self.ctx, self.args.thread_num)
        child_identities = [str(inproc_child.getsockopt_string(zmq.IDENTITY)) \
            for inproc_child in inproc_childs]

        remote_socks = [zhelpers.nonblocking_socket(self.ctx, zmq.PAIR) \
            for i in xrange(self.args.thread_num)]

        if ports:
            for i, sock in enumerate(remote_socks):
                sock.connect('tcp://%s:%s' % (self.remote_ip, ports[i]))
            
        else:
            ports = []
            for i, sock in enumerate(remote_socks):
                port = zhelpers.bind_to_random_port(sock)
                ports.append(port)

            self.remote.set_remote_ports(ports)

        if self.sender:
            for i, sock in enumerate(remote_socks):
                self.childs.append(
                    SendThread(self.ctx, ports[i], sock, 
                        inproc_childs[i], self.args.timeout, self.args.pipeline,
                        self.args.chunksize, self.src.path, self.file_queue, 
                        self.args.compress)
                    )
        else:
            for i, sock in enumerate(remote_socks):
                self.childs.append(
                    RecvThread(self.ctx, ports[i], sock,
                        inproc_childs[i], self.args.timeout, self.args.pipeline,
                        self.args.chunksize, self.dst.path, self.args.compress)
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
            MYLOGGER.critical('prepare failed')
            return

        MYLOGGER.debug('%s runing' % self.__class__.__name__)

        while not self.stoped:
            try:
                polls = self.poll(1000)
                self.deal_poll(polls)

                progress = []
                for child in self.childs:
                    if child.file.file:
                        progress.append((child.file.path, child.file.offset, child.file.total))

                if progress:
                    log_file_progress(progress)

            except KeyboardInterrupt:
                self.log(logging.INFO, 'user interrupted, exit')
                self.stop()
                if self.remote:
                    self.remote.do_stop()

            if not self.check_timeout():
                MYLOGGER.info('timeout, exit')
                self.stop()
                break

            if self.childs and not self.has_child_alive():
                MYLOGGER.info('%s all thread stop, exit' % self.__class__.__name__)
                self.stop()
                break

        return
