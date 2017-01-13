# -*- coding: utf-8 -*-

import sys
import time
import zmq

def run():
    context = zmq.Context()

    receiver = context.socket(zmq.PULL)
    receiver.bind('tcp://*:5558')

    s = receiver.recv()

    tstart = time.time()

    for task_nbr in xrange(100):
        s = receiver.recv()
        if task_nbr % 10 == 0:
            sys.stdout.write(':')
        else:
            sys.stdout.write('.')
        sys.stdout.flush()

    tend = time.time()
    print 'Total elapsed time: %d msec' % ((tend-tstart)*1000)

if __name__ == '__main__':
    run()
