# -*- coding: utf-8 -*-

import zmq

def run():
    ctx = zmq.Context()
    dealer = ctx.socket(zmq.DEALER)
    router = ctx.socket(zmq.ROUTER)

    dealer.bind('tcp://*:5556')
    router.bind('tcp://*:5555')

    zmq.proxy(router, dealer)
    return

if __name__ == '__main__':
    run()