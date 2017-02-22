# -*- coding: utf-8 -*-

import logging
import zmq
import zhelpers
from collections import deque
import subprocess
import config
import zsync_network
import zsync_utils
from zsync_thread import FileTransciver


class ZsyncDaemon(zsync_network.Transceiver):
    def __init__(self, port):
        super(ZsyncDaemon, self).__init__()
        self.ctx = zmq.Context()
        self.port = port
        self.sock = None
        self.waiting_clients = deque()
        self.services = []
        return

    def _prepare(self):
        self.sock = zhelpers.nonblocking_socket(self.ctx, zmq.ROUTER)

        try:
            self.sock.bind('tcp://*:%s' % self.port)
        except Exception as e:
            logging.critical(str(e))
            return False

        self.register(self.sock)
        return True

    def run(self):
        if not self._prepare():
            return

        logging.info('zsync daemon mode started.')
        while True:
            polls = self.poll(1000)
            self.deal_poll(polls)
        return

    def on_new_client(self, client, src, dst, thread_num,
            timeout, pipeline, chunksize, excludes):

        if len(self.services) > config.DAEMON_MAX_SUBPROCESS:
            client.new_client_failed('too many clients, please try later.')
            return

        src = zsync_utils.CommonPath(src)
        dst = zsync_utils.CommonPath(dst)

        if not src.isLocal() and not src.visitValid():
            client.new_client_failed('remote path is invalid.')
            return

        if not src.isLocal() and not src.visitValid():
            client.new_client_failed('remote path is invalid.')
            return

        self.waiting_clients.append(client)
        sub_args = ['python', 'zsync.py', src.full(), dst.full(), \
            '--remote', '--port', str(self.port), \
            '--thread-num', str(thread_num), \
            '--timeout', str(timeout), \
            '--pipeline', str(pipeline), \
            '--chunksize', str(chunksize)]

        if excludes:
            for exclude in excludes:
                sub_args.extend(['--exclude', str(exclude)])

        logging.debug('creating subprocess %s' % sub_args)
        sub = subprocess.Popen(sub_args)
        self.services.append(sub)
        return

    def on_client_exit(self, client, msg):
        if client in self.waiting_clients:
            self.waiting_clients.remove(client)
            logging.warning('client %s exit: %s' % (client.identity, msg))
        return

    def on_new_service(self, service, port):
        if not self.waiting_clients:
            service.exit()
            return

        client = self.waiting_clients.popleft()
        client.on_new_serivce(port)
        return

class ZsyncRemoteService(FileTransciver):
    def __init__(self, src_path, dst_path, daemon_port, pipeline=0, 
            chunksize=0, thread_num=0, 
            timeout=10, excludes=None):

        ctx = zmq.Context()

        FileTransciver.__init__(self, ctx, src_path, dst_path, 
            pipeline, chunksize, thread_num, timeout, excludes)

        self.daemon_sock = None            # 跟 daemon 的 sock
        self.daemon_port = daemon_port
        self.daemon = None
        return

    def _prepare(self):
        if not self.src.isLocal():
            self.sender = True

        self.remote_sock = zhelpers.nonblocking_socket(self.ctx, zmq.PAIR)
        self.remote_port = zhelpers.bind_to_random_port(self.remote_sock)
        if not self.remote_port:
            logging.critical('service failed to bind random port')
            return False
        self.remote = zsync_network.Proxy(self, self.remote_sock)

        self.daemon_sock = zhelpers.nonblocking_socket(self.ctx, zmq.DEALER)
        self.daemon_sock.connect('tcp://localhost:%s' % self.daemon_port)
        self.daemon = zsync_network.Proxy(self, self.daemon_sock)

        self.register(self.remote_sock)
        self.register(self.daemon_sock)

        self.daemon.on_new_service(self.remote_port)
        logging.info('zsync remote service started.')
        return True

    def begin_sync(self, remote):
        if self.sender and not self.prepare_sender():
            self.stop()
            remote.do_stop('remote prepare sender failed.')
            return

        self.create_childs()
        return

class ZsyncLocalService(FileTransciver):
    def __init__(self, src_path, dst_path, client_port, pipeline=0, 
            chunksize=0, thread_num=0, 
            timeout=10, excludes=None):

        ctx = zmq.Context()

        FileTransciver.__init__(self, ctx, src_path, dst_path, 
            pipeline, chunksize, thread_num, timeout, excludes)

        self.ctx = zmq.Context()
        self.remote_port = client_port
        self.sender = True          # local service fix to be sender
        return

    def _prepare(self):
        self.remote_sock = zhelpers.nonblocking_socket(self.ctx, zmq.PAIR)
        self.remote_sock.connect('tcp://localhost:%s' % self.remote_port)
        self.remote = zsync_network.Proxy(self, self.remote_sock)

        self.register(self.remote_sock)
        self.add_timeout(self.remote_sock, self.timeout)

        if not self.prepare_sender():
            return False

        self.remote.shake_hand()
        self.create_childs()
        logging.debug('local service started')
        return True


class ZsyncClient(FileTransciver):
    def __init__(self, src_path, dst_path, daemon_port=5555,
            pipeline=0, chunksize=0, thread_num=3,
            timeout=10, excludes=None):

        ctx = zmq.Context()

        FileTransciver.__init__(self, ctx, src_path, dst_path, 
            pipeline, chunksize, thread_num, timeout, excludes)

        self.daemon_sock = None                    # 跟 daemon 的 sock
        self.daemon_port = daemon_port
        self.daemon = None
        return

    def _prepare(self):
        if not self.src.isValid():
            logging.error('src path is invalid')
            return False

        if not self.dst.isValid():
            logging.error('dst path is invalid')
            return False

        if not self.src.isLocal() and not self.dst.isLocal():
            logging.error('src and dst cannot be both remote address')
            return False

        # need local service
        if self.src.isLocal() and self.dst.isLocal():
            self.remote_ip = 'localhost'
            self.remote_sock = zhelpers.nonblocking_socket(self.ctx, zmq.PAIR)
            self.remote_port = zhelpers.bind_to_random_port(self.remote_sock)
            if not self.remote_port:
                logging.critical('failed to bind random to random port')
                return False

            sub_args = ['python', 'zsync.py', self.src.full(),
                self.dst.full(), '--local', '--port', str(self.remote_port),
                '--pipeline', str(self.pipeline), '--chunksize', str(self.chunksize),
                '--thread-num', str(self.thread_num)]

            if self.excludes:
                for exclude in self.excludes.excludes_origin:
                    sub_args.extend(['--exclude', str(exclude)])

            logging.info('creating subprocess %s' % sub_args)
            sub = subprocess.Popen(sub_args)

            self.register(self.remote_sock)
            self.add_timeout(self.remote_sock, self.timeout)
            self.remote = zsync_network.Proxy(self, self.remote_sock)

        # need remote service
        else:
            if not self.src.isLocal():
                self.remote_ip = self.src.ip
            else:
                self.remote_ip = self.dst.ip
                self.sender = True

            remote_addr = 'tcp://%s:%s' % (self.remote_ip, self.daemon_port)

            self.daemon_sock = zhelpers.nonblocking_socket(self.ctx, zmq.DEALER)
            self.daemon_sock.connect(remote_addr)
            self.register(self.daemon_sock)
            self.add_timeout(self.daemon_sock, self.timeout)

            if self.excludes:
                excludes = self.excludes.excludes_origin
            else:
                excludes = None

            self.daemon = zsync_network.Proxy(self, self.daemon_sock)
            self.daemon.on_new_client(self.src.full(), self.dst.full(),
                self.thread_num, self.timeout, self.pipeline,
                self.chunksize, excludes)

        logging.debug('client started')
        return True

    def on_new_serivce(self, daemon, port):
        self.remote_sock = zhelpers.nonblocking_socket(self.ctx, zmq.PAIR)
        remote_addr = 'tcp://%s:%s' % (self.remote_ip, port)
        self.remote_sock.connect(remote_addr)

        self.del_timeout(self.daemon_sock)
        self.register(self.remote_sock)
        self.add_timeout(self.remote_sock, self.timeout)
        self.remote = zsync_network.Proxy(self, self.remote_sock)   
        self.remote.shake_hand()

        if self.sender and not self.prepare_sender():
            self.stop()
            self.remote.do_stop('remote prepare sender failed.')
            return

        self.remote.begin_sync()
        return

