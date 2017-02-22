# -*- coding: utf-8 -*-

import zmq
import os
import time
from threading import Thread
from zhelpers import socket_set_hwm, zpipe
from multiprocessing import Queue

fileq = None
mypath = 'sync_files'
ip = 'localhost'
ports = [13330, 13331, 13332]
CHUNK_SIZE = 250000
PIPELINE = 10

def send_one(socket):
    if fileq.empty():
        return False

    try:
        fname = fileq.get(True, 0.001)
    except Queue.Empty, e:
        return True

    try:
        msg = socket.recv_multipart()
    except zmq.ZMQError as e:
        if e.errno == zmq.ETERM:
            return False   # shutting down, quit
        else:
            raise
    identity, command = msg
    assert command == b"fetch"
    socket.send_multipart([identity, fname])

    print 'sending ' + fname

    file = open(fname, 'r')

    while True:
        try:
            msg = socket.recv_multipart()
        except zmq.ZMQError as e:
            if e.errno == zmq.ETERM:
                return False   # shutting down, quit
            else:
                raise
    
        identity, command, offset_str, chunksz_str = msg

        if command == b'close':
            break

        assert command == b"fetch"
        offset = int(offset_str)
        chunksz = int(chunksz_str)

        # Read chunk of data from file
        file.seek(offset, os.SEEK_SET)
        data = file.read(chunksz)

        # Send resulting chunk to client
        socket.send_multipart([identity, offset_str, data])

    file.close()
    return True

def server_thread(ctx, port):
    router = ctx.socket(zmq.ROUTER)
    socket_set_hwm(router, PIPELINE * 2)
    tcp = "tcp://*:%d" % port
    router.bind(tcp)
    print 'binding %s \n' % tcp
    
    while True:
        ret = send_one(router)
        if not ret:
            break

    return

if __name__ == '__main__':
    begint = time.time()
    onlyfiles = [os.path.join(mypath, f) for f in os.listdir(mypath) if os.path.isfile(os.path.join(mypath, f)) ]

    fileq = Queue(len(onlyfiles))
    for f in onlyfiles:
        fileq.put_nowait(f)

    ctx = zmq.Context()
    servers = [Thread(target=server_thread, args=(ctx, port,)) for port in ports]
    [server.start() for server in servers]
    [server.join() for server in servers]
    endt = time.time()

    print 'finish: %s' % (endt - begint)