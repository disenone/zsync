# -*- coding: utf-8 -*-

import zmq
import os
import time
from threading import Thread
from zhelpers import socket_set_hwm, zpipe
from zsync_server import CHUNK_SIZE, PIPELINE

ip = '192.168.1.107'
ports = [13330, 13331, 13332]


def client_thread(ctx, port):
    dealer = ctx.socket(zmq.DEALER)
    socket_set_hwm(dealer, PIPELINE)
    dealer.connect('tcp://%s:%d' % (ip, port))

    credit = PIPELINE   # Up to PIPELINE chunks in transit

    total = 0           # Total bytes received
    chunks = 0          # Total chunks received
    offset = 0          # Offset of next chunk request

    dealer.send_multipart([b'fetch'])

    try:
        fname = dealer.recv()
    except zmq.ZMQError as e:
        if e.errno == zmq.ETERM:
            return   # shutting down, quit
        else:
            raise

    outf = open(fname, 'w')

    while True:
        while credit:
            # ask for next chunk
            dealer.send_multipart([
                b"fetch",
                b"%i" % total,
                b"%i" % CHUNK_SIZE,
            ])

            offset += CHUNK_SIZE
            credit -= 1

        try:
            chunk = dealer.recv()
        except zmq.ZMQError as e:
            if e.errno == zmq.ETERM:
                return   # shutting down, quit
            else:
                raise

        outf.write(chunk)

        chunks += 1
        credit += 1
        size = len(chunk)
        total += size
        if size < CHUNK_SIZE:
            break   # Last chunk received; exit

    print ("%i chunks received, %i bytes" % (chunks, total))
    pipe.send(b"OK")
    return

if __name__ == '__main__':
    begint = time.time()
    ctx = zmq.Context()
    clients = [Thread(target=client_thread, args=(ctx, port,)) for port in ports]
    [client.start() for client in clients]
    [client.join() for client in clients]
    endt = time.time()
    print 'finish: %ss' % (endt - begint)