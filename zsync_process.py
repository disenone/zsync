# -*- coding: utf-8 -*-

import logging
import zmq
import zsync_thread
import zhelpers
from collections import deque
import subprocess
import config
import zsync_network
import zsync_utils


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
        return

    def run(self):
        if not self._prepare():
            return

        logging.info('zsync daemon mode started.')
        while True:
            polls = self.poll(1000)
            self.deal_poll(polls)
        return

    def on_new_client(self, client):
        if len(self.services) > config.DAEMON_MAX_SUBPROCESS:
            client.new_client_failed()
            return

        self.waiting_clients.append(client)
        sub_args = ['python', 'zsync.py', '--remote', '--port', str(self.port)]
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

class ZsyncRemoteService(zsync_network.Transceiver):
    def __init__(self, port, timeout):
        super(ZsyncRemoteService, self).__init__()
        self.ctx = zmq.Context()
        self.sock = None                    # 跟客户端的 sock
        self.port = None
        self.process_sock = None            # 跟 daemon 的 sock
        self.process_port = port
        self.parent = None
        self.timeout = timeout
        return

    def _prepare(self):
        self.sock = zhelpers.nonblocking_socket(self.ctx, zmq.PAIR)

        self.port = zhelpers.bind_to_random_port(self.sock)
        if not self.port:
            logging.critical('service failed to bind random port')
            return False

        self.process_sock = zhelpers.nonblocking_socket(self.ctx, zmq.DEALER)
        self.process_sock.connect('tcp://localhost:%s' % self.process_port)
        self.parent = zsync_network.Proxy(self, self.process_sock)

        self.register(self.sock)
        self.register(self.process_sock)

        return True

    def run(self):
        if not self._prepare():
            return

        self.parent.on_new_service(self.port)

        logging.info('zsync remote service started.')
        while True:
            polls = self.poll(1000)
            self.deal_poll(polls)
        return

    def on_client_msg(self, client, msg):
        logging.info('client msg: %s' % msg)
        return

class ZsyncLocalService(zsync_network.Transceiver):
    def __init__(self, port, timeout):
        super(ZsyncLocalService, self).__init__()
        self.ctx = zmq.Context()
        self.sock = None                    # 跟客户端的 sock
        self.port = port
        self.client = None
        self.timeout = timeout
        return

    def _prepare(self):
        self.sock = zhelpers.nonblocking_socket(self.ctx, zmq.PAIR)
        self.sock.connect('tcp://localhost:%s' % self.port)
        self.client = zsync_network.Proxy(self, self.sock)

        self.register(self.sock)
        self.add_timeout(self.sock, self.timeout)
        return True

    def run(self):
        if not self._prepare():
            return

        logging.debug('local service running')

        self.client.on_local_service()

        while True:
            polls = self.poll(1000)
            self.deal_poll(polls)

            if not self.check_timeout():
                return
        return


class ZsyncClient(zsync_network.Transceiver):
    def __init__(self, src_path, dst_path, daemon_port=5555,
            thread_num=3, timeout=10, excludes=None):

        super(ZsyncClient, self).__init__()
        self.ctx = zmq.Context()
        self.daemon_sock = None                    # 跟 daemon 的 sock
        self.daemon_port = daemon_port
        self.daemon = None
        self.service_sock = None
        self.service_port = None
        self.service = None
        self.src = src_path
        self.dst = dst_path
        self.thread_num = thread_num
        self.timeout = timeout
        self.excludes = excludes
        self.remote_ip = None
        return

    def _prepare(self):
        self.src = zsync_utils.CommonPath(self.src)
        self.dst = zsync_utils.CommonPath(self.dst)

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
            self.service_sock = zhelpers.nonblocking_socket(self.ctx, zmq.PAIR)
            self.service_port = zhelpers.bind_to_random_port(self.service_sock)
            if not self.service_port:
                logging.critical('failed to bind random to random port')
                return False

            sub_args = ['python', 'zsync.py', self.src.full(),
                self.dst.full(), '--local', '--port', str(self.service_port)]
            logging.info('creating subprocess %s' % sub_args)
            sub = subprocess.Popen(sub_args)

            self.register(self.service_sock)
            self.add_timeout(self.service_sock, self.timeout)
            self.service = zsync_network.Proxy(self, self.service_sock)

        # need remote service
        else:
            if not self.src.isLocal():
                self.remote_ip = self.src.ip
            else:
                self.remote_ip = self.dst.ip

            remote_addr = 'tcp://%s:%s' % (self.remote_ip, self.daemon_port)

            self.daemon_sock = zhelpers.nonblocking_socket(self.ctx, zmq.DEALER)
            self.daemon_sock.connect(remote_addr)
            self.register(self.daemon_sock)
            self.add_timeout(self.daemon_sock, self.timeout)

            self.daemon = zsync_network.Proxy(self, self.daemon_sock)
            self.daemon.on_new_client()

        return True

    def run(self):
        if not self._prepare():
            return

        logging.debug('client running')

        while True:
            polls = self.poll(1000)
            self.deal_poll(polls)

            if not self.check_timeout():
                return
        return

    def on_new_serivce(self, daemon, port):
        self.service_sock = zhelpers.nonblocking_socket(self.ctx, zmq.PAIR)
        remote_addr = 'tcp://%s:%s' % (self.remote_ip, port)
        self.service_sock.connect(remote_addr)

        self.del_timeout(self.daemon_sock)
        self.register(self.service_sock)
        self.add_timeout(self.service_sock, self.timeout)
        self.service = zsync_network.Proxy(self, self.service_sock)   

        self.service.on_client_msg('hello world')
        return

    def on_local_service(self, service):
        logging.debug('recv local service')
        self.service = service

        
        return
