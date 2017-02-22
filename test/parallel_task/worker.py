# -*- coding: utf-8 -*-

import sys
import time
import zmq

def run():
    context = zmq.Context()

    receiver = context.socket(zmq.PULL)
    receiver.connect('tcp://localhost:5557')

    sender = context.socket(zmq.PUSH)
    sender.connect('tcp://localhost:5558')

    for i in xrange(100):
        s = receiver.recv()

        sys.stdout.write('.')
        sys.stdout.flush()

        time.sleep(int(s) * 0.001)

        sender.send(b'')

if __name__ == '__main__':
    run()