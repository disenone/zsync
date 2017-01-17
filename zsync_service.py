# -*- coding: utf-8 -*-

import zmq
import argparse

def run():
    ctx = zmq.Context()
    dealer = ctx.socket(zmq.ROUTER)

    dealer.connect('tcp://localhost:5556')

    while True:
        msg = dealer.recv_multipart()
        print msg
        dealer.send_multipart([msg[0], msg[1], 'reply'])
        dealer.send_multipart([msg[0], msg[1], 'abc'])
        dealer.send_multipart([msg[0], msg[1], 'def'])
    return

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    args = parser.parse_args()
    run()