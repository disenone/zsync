# -*- coding: utf-8 -*-

import zmq
import cPickle
import logging
from collections import deque
import zhelpers


class RpcCaller(object):
    def __init__(self, transceiver, sock, funcName, identity=None):
        self.transceiver = transceiver
        self.sock = sock
        self.funcName = funcName
        self.identity = identity
        return

    def __call__(self, *args):
        args = [cPickle.dumps(args)]
        if self.identity:
            args = self.identity + args
        self.transceiver.send(self.sock, args)
        return


class Proxy(object):
    def __init__(self, transceiver, sock, identity=None):
        self.transceiver = transceiver
        self.sock = sock
        self.identity = identity
        return

    def __getattr__(self, name):
        return RpcCaller(self.transceiver, self.sock, name, self.identity)


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
        self.cmd_pos = {}
        return

    def register(self, sock, cmd_pos=0):
        self.in_poller.register(sock, zmq.POLLIN)
        self.all_poller.register(sock)
        if cmd_pos:
            self.cmd_pos[sock] = cmd_pos
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
        identity = None
        if sock.socket_type == zmq.ROUTER:
            identity, msg = zhelpers.split_identity(msg)

        funcn = msg[0]

        func = getattr(self, funcn, None)
        if not func:
            logging.error('invalid function "%s"' % msg)
            return

        try:
            args = cPickle.loads(msg[1])
        except Exception as e:
            logging.error('invalid function args: %s' % msg)

        return func(*args)

    def deal_poll(self, polls):
        if not polls:
            return

        for sock, state in polls.iteritems():
            if zmq.POLLIN in state:
                while True:
                    try:
                        msg = self.recv(sock)
                    except Exception, e:
                        break
                    self.dispatch(sock, msg)

            if zmq.POLLOUT in state:
                self.sendQueue(sock)
        return