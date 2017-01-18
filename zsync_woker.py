# -*- coding: utf-8 -*-

import zmq
import sys
import argparse
import zsync_utils
import threading


def run(args):
    src = zsync_utils.CommonFile(args.src)
    dst = zsync_utils.CommonFile(args.dst)

    if not src.isValid():
        print 'ERROR: src is invalid'
        return

    if not dst.isValid():
        print 'ERROR: dst is invalid'
        return

    thread_num = args.thread_num

    print 'src = %s\ndst = %s\n' % (src, dst)

    ctx = zmq.Context()
    dealer = ctx.socket(zmq.DEALER)
    dealer.connect('tcp://%s:%s' % (src.ip, args.p))

    poller = zmq.Poller()
    poller.register(dealer, zmq.POLLIN)

    dealer.send_multipart([src.path, str(thread_num)])

    while True:
        try:
            socks = dict(poller.poll(1000))

            if socks.get(dealer) == zmq.POLLIN:
                msg = dealer.recv_multipart(zmq.NOBLOCK)
                print msg
        except KeyboardInterrupt:
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

    args = parser.parse_args()

    # print args
    # parser.print_help()
    run(args)
