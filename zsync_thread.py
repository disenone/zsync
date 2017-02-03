# -*- coding: utf-8 -*-

import zmq

class ZsyncThread(threading.Thread):
    def __init__(self):
        self.sock = None
        raise NotImplementedError

    def run(self):
        raise NotImplementedError

    def send(self):
        raise NotImplementedError


class SendThread(threading.Thread):
    def __init__(self, ctx, thread_id, send_queue):
        threading.Thread.__init__(self)  
        self.ctx = ctx
        self.thread_id = thread_id
        self.send_queue = send_queue
        self.stoped = False
        self.ready = False

        # create pair socket to send file
        self.sock = ctx.socket(zmq.PAIR)
        port = self.sock.bind_to_random_port('tcp://*', min_port=10000, max_port=11000, max_tries=1000)
        if not port:
            return
        self.sock.linger = 0
        self.port = port
        self.ready = True
        return

    def run(self):
        if not self.ready:
            return

        poller = zmq.Poller()
        poller.register(self.sock)

        while True:
            print 'thread %d, ' % self.thread_id,  self.stoped
            if self.stoped:
                break

            print 'go in poll'
            socks = dict(poller.poll(1000))
            if socks.get(self.sock) & zmq.POLLIN == zmq.POLLIN:
                msg = self.sock.recv_multipart(zmq.NOBLOCK)
                print msg
                if not msg:
                    raise ValueError('receive empty msg')
                    break
            print 'go out poll'

            if socks.get(self.sock) & zmq.POLLOUT == zmq.POLLOUT:
                print 'before send'
                self.send('test', 'hello world!')
                print 'sending msg'

        print 'thread %d exit' % self.thread_id

        return

    def stop(self):
        self.stoped = True
        #self.sock.close()
        print 'stoped'
        return

    def send(self, *msgs):
        self.sock.send_multipart(msgs, zmq.NOBLOCK)
        return

class RecvThread(threading.Thread):
    def __init__(self, ctx, ip, port):
        threading.Thread.__init__(self)
        self.ctx = ctx
        self.ip = ip
        self.port = port
        self.ready = False
        self.stoped = False

        # create pair socket to recv file
        self.sock = ctx.socket(zmq.PAIR)
        self.sock.connect('tcp://%s:%s' % (self.ip, self.port))
        self.sock.linger = 0
        self.ready = True
        return

    def run(self):
        poller = zmq.Poller()
        poller.register(self.sock, zmq.POLLIN)

        num = 0
        while True:
            if self.stoped:
                break

            socks = dict(poller.poll(1000))
            if socks.get(self.sock) == zmq.POLLIN:
                msg = self.sock.recv_multipart(zmq.NOBLOCK)
                if not msg:
                    break
                print msg
                print 'before send'
                self.sock.send_multipart(msg, zmq.NOBLOCK)
                print 'send end'
                num += 1
                if num > 10:
                    self.sock.close()
                    break
        return

    def send(self, *msgs):
        self.sock.send_multipart(msgs)
        return

    def stop(self):
        self.stoped = True
        return