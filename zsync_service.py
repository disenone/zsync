# -*- coding: utf-8 -*-

import zmq

def run():
    ctx = zmq.Context()
    dealer = ctx.socket(zmq.ROUTER)

    dealer.connect('tcp://localhost:5556')

    while True:
        msg = dealer.recv_multipart()
        print msg
        dealer.send_multipart([msg[0], msg[1], 'reply'])
    return

if __name__ == '__main__':
    run()