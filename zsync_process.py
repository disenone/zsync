# -*- coding: utf-8 -*-

import logging
import zmq
import zsync_thread
import zhelpers
from collections import deque
import subprocess
import config


class ZsyncDaemon(zsync_thread.Transceiver):
    def __init__(self, port):
        super(ZsyncDaemon, self).__init__()
        self.ctx = zmq.Context()
        self.port = port
        self.sock = None
        self.waiting_clients = deque()
        self.services = []
        return

    def prepare(self):
        self.sock = zhelpers.nonblocking_socket(self.ctx, zmq.ROUTER)

        try:
            self.sock.bind('tcp://*:%s' % self.port)
        except Exception as e:
            logging.critical(str(e))
            return False

        self.register(self.sock)
        return

    def run(self):
        if not self.prepare():
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

        self.waiting_clients.append(client.identity)
        sub_args = ['python', 'zsync.py', '--remote', '--port', str(self.port)]
        logging.debug('creating subprocess %s' % sub_args)
        sub = subprocess.Popen(sub_args)
        self.services.append(sub)
        return

    def on_client_exit(self, client, msg):
        if client.identity in self.waiting_clients:
            self.waiting_clients.remove(client.identity)
            logging.warning('client %s exit: %s' % (client.identity, msg))
        return

    def on_new_service(self, service, port):
        return

