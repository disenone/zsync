# -*- coding: utf-8 -*-

import zmq
import cPickle
from zsync_logger import MYLOGGER as logging
from collections import deque
import zhelpers
import time


class RpcCaller(object):
    def __init__(self, transceiver, sock, funcName, identity=None):
        self.transceiver = transceiver
        self.sock = sock
        self.funcName = funcName
        self.identity = identity
        return

    def __call__(self, *args):
        args = [self.funcName, cPickle.dumps(args)]
        if self.identity:
            args = self.identity + args
        self.transceiver.send(self.sock, *args)
        return

class Proxy(object):
    def __init__(self, transceiver, sock, identity=None):
        self.transceiver = transceiver
        self.sock = sock

        if type(identity) is str:
            identity = [identity]
        elif identity is not None:
            if not type(identity) in [tuple, list]:
                raise ValueError('identity is invalid: %s' % identity)
            identity = list(identity)
        self.identity = identity
        return

    def __getattr__(self, name):
        return RpcCaller(self.transceiver, self.sock, name, self.identity)

    def __cmp__(self, other):
        return cmp((self.identity, self.sock, self.transceiver),
            (other.identity, other.sock, other.transceiver))

    def __str__(self):
        return str(self.identity)

    def call_raw(self, name, *args):
        args = (zhelpers.RAW_MSG_FLAG, name) + args
        if self.identity:
            args = tuple(self.identity) + args
        self.transceiver.send(self.sock, *args)
        return


class SendQueue(dict):
    def push_queue(self, sock, msg):
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
        if not queue:
            self.pop(sock, None)
        return

class Transceiver(object):
    def __init__(self):
        self.send_queue = SendQueue()
        self.in_poller = zmq.Poller()
        self.all_poller = zmq.Poller()
        self.timeout_checkers = {}
        return

    def register(self, sock):
        self.in_poller.register(sock, zmq.POLLIN)
        self.all_poller.register(sock)
        return

    def delay_all_timeout(self):
        [checker.feed() for checker in self.timeout_checkers.itervalues()]
        return

    def add_timeout(self, sock, timeout):
        self.timeout_checkers[sock] = TimeoutChecker(timeout)
        return

    def del_timeout(self, sock):
        self.timeout_checkers.pop(sock, None)
        return

    def check_timeout(self):
        if not self.timeout_checkers:
            return True

        any_timeout = any([checker.timeout() for checker in self.timeout_checkers.itervalues()])
        if any_timeout:
            MYLOGGER.error('connect timeout')
            return False

        return True

    def send(self, sock, *msg):
        if not msg:
            return
        if not self.send_queue:
            try:
                sock.send_multipart(msg, zmq.NOBLOCK)
                # MYLOGGER.debug('sended: %s' % (msg, ))
                return True
            except Exception as e:
                # MYLOGGER.debug('send error: %s, %s' % (e, msg))
                self.send_queue.push_queue(sock, msg)
        else:
            self.send_queue.push_queue(sock, msg)


        return False

    def queue_send(self, sock):
        self.send_queue.send(sock)
        return

    def recv(self, sock):
        msg = sock.recv_multipart(zmq.NOBLOCK)
        if sock in self.timeout_checkers:
            self.timeout_checkers[sock].feed()
        return msg

    def poll(self, ms):
        if self.send_queue:
            return zhelpers.poll(self.all_poller, ms)
        else:
            return zhelpers.poll(self.in_poller, ms)

    def deal_poll(self, polls):
        if not polls:
            return

        for sock, state in polls.iteritems():
            if zmq.POLLIN in state:
                while True:
                    try:
                        msg = self.recv(sock)
                        #MYLOGGER.debug('recved :%s' % msg)
                    except Exception, e:
                        break
                    self.dispatch(sock, msg)

            if zmq.POLLOUT in state:
                self.queue_send(sock)
        return

    def dispatch(self, sock, msg):
        identity = None

        if sock.socket_type == zmq.ROUTER:
            identity, msg = zhelpers.split_identity(msg)

        is_raw = msg[0] == zhelpers.RAW_MSG_FLAG
        if is_raw:
            msg = msg[1:]

        funcn = msg[0]

        func = getattr(self, funcn, None)
        if not func:
            MYLOGGER.error('not found function "%s"' % msg)
            return

        try:
            if is_raw:
                args = msg[1:]
            else:
                args = cPickle.loads(msg[1])
        except Exception as e:
            MYLOGGER.error('invalid function args: %s' % msg)
            return

        proxy = Proxy(self, sock, identity)

        return func(proxy, *args)


class TimeoutChecker(object):
    def __init__(self, timeout):
        self.interval = timeout
        self.timestamp = time.time()
        return

    def feed(self):
        self.timestamp = time.time()
        return

    def timeout(self):
        return time.time() > self.timestamp + self.interval
