# -*- coding: utf-8 -*-

import zmq
import argparse

def run(args):
    ctx = zmq.Context()
    dealer = ctx.socket(zmq.ROUTER)

    try:
        dealer.connect('tcp://%s:%s' % (args.ip, args.p))
    except:
        raise
        return

    while True:
        msg = dealer.recv_multipart()
        print msg
        dealer.send_multipart([msg[0], msg[1], 'reply'])
    return

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-ip', type=str, help='broker ip', default='localhost')
    parser.add_argument('-p', type=str, help='port', default='5556')
    args = parser.parse_args()
    run(args)
