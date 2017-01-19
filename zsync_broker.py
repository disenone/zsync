# -*- coding: utf-8 -*-

import zmq
import argparse

def run(args):
    ctx = zmq.Context()
    dealer = ctx.socket(zmq.DEALER)
    router = ctx.socket(zmq.ROUTER)

    if args.sp == args.cp:
        print 'ERROR: service port must be different from client port'
        return

    dealer.bind('tcp://*:%s' % args.sp)
    router.bind('tcp://*:%s' % args.cp)

    try:
        zmq.proxy(router, dealer)
    except:
        print 'except'
        raise
    return

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-sp', type=str, help='service port', default='5556')
    parser.add_argument('-cp', type=str, help='client port', default='5555')
    args = parser.parse_args()
    run(args)