# -*- coding: utf-8 -*-

import sys
import argparse
import zsync_utils
import threading
import time
import cPickle
import zmq

CHUNK_SIZE = 250000
PIPELINE = 10

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


def run(args):
    src = zsync_utils.CommonFile(args.src)
    dst = zsync_utils.CommonFile(args.dst)

    if not src.isValid():
        print 'ERROR: src is invalid'
        return

    if not dst.isValid():
        print 'ERROR: dst is invalid'
        return

    if args.timeout <= 0:
        print 'ERROR: timeout is invalid, must be > 0'
        return

    if args.thread_num <= 0:
        print 'ERROR: thread_num is invalid, must be > 0'
        return

    thread_num = args.thread_num

    print 'src = %s\ndst = %s\n' % (src.full(), dst.full())

    ctx = zmq.Context()
    dealer = ctx.socket(zmq.DEALER)
    dealer.connect('tcp://%s:%s' % (src.ip, args.p))

    # 退出时不等待发送缓冲区发送数据，不设置会很有可能导致程序无法退出 
    # http://stackoverflow.com/questions/7939977/zeromq-with-python-hangs-if-connecting-to-invalid-socket
    dealer.linger = 0  

    poller = zmq.Poller()
    poller.register(dealer, zmq.POLLIN)

    dealer.send_multipart([src.path, str(thread_num)])

    # 10秒的连接时间，如果超过10秒都没有收到一次数据，判断连接不上
    connected = False
    connect_time = time.time()

    threads = []
    
    while True:
        try:
            socks = dict(poller.poll(1000))

            if socks.get(dealer) == zmq.POLLIN:
                msg = dealer.recv_multipart(zmq.NOBLOCK)
                if not msg:
                    continue

                connected = True
                print msg
                command = msg[0]

                if command == 'error':
                    print 'ERROR: %s' % msg[1]
                    break
                elif command == 'port':
                    ports = cPickle.loads(msg[1])
                    print 'ports: ', ports

                    threads = [RecvThread(ctx, src.ip, port) for port in ports]
                    [thread.start() for thread in threads]

            if not connected:
                if time.time() - connect_time >= args.timeout:
                    print 'connect timeout, exit'
                    break

        except KeyboardInterrupt:
            for thread in threads:
                thread.stop()
            print 'user interrupted, exit'
            break
    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('src', type=str, help='src', default='')
    parser.add_argument('dst', type=str, help='dst', default='')
    parser.add_argument('-p', type=str, help='port', default='5555')
    parser.add_argument('--thread-num', type=int, default=3, help='sync thread num')
    parser.add_argument('--exclude', type=str, action='append', help='exclude file or directory to sync')
    parser.add_argument('--timeout', type=int, default=5, help='connect timeout second')

    args = parser.parse_args()

    # print args
    # parser.print_help()
    run(args)
