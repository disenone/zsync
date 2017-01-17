# -*- coding: utf-8 -*-

import zmq
import sys

def run():
    ctx = zmq.Context()
    dealer = ctx.socket(zmq.DEALER)

    dealer.connect('tcp://localhost:5555')

    while True:
        msg = raw_input('input: ')
        dealer.send_multipart([msg])
        msg = dealer.recv_multipart()
        print msg
    return

if __name__ == '__main__':
    run()