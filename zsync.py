# -*- coding: utf-8 -*-

import argparse
import time
import sys
import zmq
import zsync_thread
import zsync_utils
import zhelpers
import subprocess


def run(args):
    ctx = zmq.Context()

    if args.daemon:
        sock = zhelpers.nonblocking_socket(ctx, zmq.ROUTER)
        addr = 'tcp://*:%s' % args.port
        sock.bind(addr)

        poller = zmq.Poller()
        poller.register(sock, zmq.POLLIN)
        while True:
            polls = zhelpers.poll(poller, 1000)
            if not polls:
                continue

            if zmq.POLLIN not in polls.get(sock, []):
                continue

            msg = sock.recv_multipart(zmq.NOBLOCK)

            print msg
            sock.send_multipart(msg, zmq.NOBLOCK)

        return

    # local server
    elif args.local:
        if not args.port:
            print 'ERROR: --local should specify --port'
            return

        sock = zhelpers.nonblocking_socket(ctx, zmq.PAIR)
        addr = 'tcp://localhost:%s' % args.port
        sock.connect(addr)
        sock.send_multipart(['hello', 'world'], zmq.NOBLOCK)
        time.sleep(1)

    # remote server
    elif args.remote:
        sock = ctx.socket(zmq.PAIR)
        port = zhelpers.bind_to_random_port(sock)

    # client
    else:
        if not args.src or not args.dst:
            print 'ERROR: need src and dst'
            return

        src = zsync_utils.CommonPath(args.src)
        dst = zsync_utils.CommonPath(args.dst)

        if not src.isLocal() and not dst.isLocal():
            print 'ERROR: src and dst cannot be both remote address'
            return

        if src.isLocal() and dst.isLocal():
            sock = zhelpers.nonblocking_socket(ctx, zmq.PAIR)
            port = zhelpers.bind_to_random_port(sock)
            if not port:
                print 'ERROR: bind random failed'
                return

            sub_args = ['python', 'zsync.py', src.full(), dst.full(), '--local', '--port', str(port)]
            print 'creating subprocess %s' % sub_args
            sub = subprocess.Popen(sub_args)

            msg = zhelpers.recv_multipart_timeout(sock, 10000)
            if not msg:
                print 'ERROR: connect timeout, exit'
                return
            else:
                print msg

        else:
            sock = zhelpers.nonblocking_socket(ctx, zmq.DEALER)
            sock.send_multipart(['newclient'], zmq.NOBLOCK)

            msg = zhelpers.recv_multipart_timeout(sock, 10000)
            if not msg:
                print 'ERROR: connect timeout, exit'
                return
            else:
                print msg

    return

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--daemon', help='daemon server', action='store_true')
    parser.add_argument('--local', help='local server', action='store_true')
    parser.add_argument('--remote', help='remote server', action='store_true')
    parser.add_argument('--sender', help='remote', action='store_true')
    parser.add_argument('src', type=str, help='src', default='', nargs='?')
    parser.add_argument('dst', type=str, help='dst', default='', nargs='?')
    parser.add_argument('-p', '--port', type=str, help='port', default='5555')
    parser.add_argument('--thread-num', type=int, default=3, help='sync thread num')
    parser.add_argument('--exclude', type=str, action='append', help='exclude file or directory to sync')
    parser.add_argument('--timeout', type=int, default=5, help='connect timeout second')
    args = parser.parse_args()

    #print args

    run(args)
    return


if __name__ == '__main__':
    main()
