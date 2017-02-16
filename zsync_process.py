# -*- coding: utf-8 -*-

import logging
import zmq
import zsync_thread
import zhelpers
from collections import deque
import subprocess


class ZsyncDaemon(zsync_thread.Transceiver):
    def __init__(self, port):
        super(ZsyncDaemon, self).__init__()
        self.ctx = zmq.Context()
        self.port = port
        self.sock = None
        self.waiting_clients = deque()
        self.servers = []
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

    def cmd_newclient(self, identity):
        self.waiting_clients.append(identity)
        sub_args = ['python', 'zsync.py', '--remote', '--port', str(self.port)]
        logging.debug('creating subprocess %s' % sub_args)
        sub = subprocess.Popen(sub_args)
        return

    def cmd_exitclient(self, identity):
        if identity in self.waiting_clients:
            self.waiting_clients.remove(identity)
        return

    def cmd_newserver(self, identity, port):
        return
