# -*- coding: utf-8 -*-

import zmq
import os
import time
from threading import Thread
from zhelpers import socket_set_hwm, zpipe
from zsync_server import CHUNK_SIZE, PIPELINE, ports, ip


def client_thread(ctx, port):
    dealer = ctx.socket(zmq.DEALER)
    socket_set_hwm(dealer, PIPELINE)
    tcp = 'tcp://%s:%d' % (ip, port)
    dealer.connect(tcp)
    print 'connecting %s \n' % tcp

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

    print 'fetching %s \n' % fname

    recvd = {}

    while True:
        while credit:
            # ask for next chunk
            dealer.send_multipart([
                b"fetch",
                b"%i" % offset,
                b"%i" % CHUNK_SIZE,
            ])

            offset += CHUNK_SIZE
            credit -= 1

        try:
            msg = dealer.recv_multipart()
        except zmq.ZMQError as e:
            if e.errno == zmq.ETERM:
                return   # shutting down, quit
            else:
                raise

        offset_str, chunk = msg

        chunks += 1
        credit += 1

        roffset = int(offset_str)
        if total != roffset:
            recvd[roffset] = chunk
            print 'total %d save offset %d' % (total, roffset)
        else:
            outf.write(chunk)
            last_size = len(chunk)
            total += last_size

            for roff in sorted(recvd.keys()):
                if roff == total:
                    chunk = recvd.pop(roff)
                    outf.write(chunk)
                    last_size = len(chunk)
                    total += last_size
                else:
                    break

            if last_size < CHUNK_SIZE:
                break   # Last chunk received; exit

    outf.close()
    dealer.send_multipart([b'close', b'0', b'0'])
    print ("%i chunks received, %i bytes" % (chunks, total))
    return

if __name__ == '__main__':
    begint = time.time()
    ctx = zmq.Context()
    clients = [Thread(target=client_thread, args=(ctx, port,)) for port in ports]
    [client.start() for client in clients]
    [client.join() for client in clients]
    endt = time.time()
    print 'finish: %ss' % (endt - begint)