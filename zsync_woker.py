# -*- coding: utf-8 -*-

import zmq
import sys
import argparse

def run(args):
    ctx = zmq.Context()
    dealer = ctx.socket(zmq.DEALER)

    dealer.connect('tcp://localhost:5555')

    while True:
        msg = raw_input('input: ')
        dealer.send_multipart([msg])
        msg = dealer.recv_multipart()
        print msg
        msg = dealer.recv_multipart()
        print msg
        msg = dealer.recv_multipart()
        print msg
    return

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('from', type=str, help='from')
    parser.add_argument('to', type=str, help='to')

    parser.add_argument('--thread-num', type=int, default=3, help='sync thread num')
    parser.add_argument('--exclude', type=str, action='append', help='exclude file or directory to sync')

    args = parser.parse_args()
    print args
    parser.print_help()
    #run()
