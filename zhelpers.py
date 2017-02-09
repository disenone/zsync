# encoding: utf-8
"""
Helper module for example applications. Mimics ZeroMQ Guide's zhelpers.h.
"""
from __future__ import print_function

import binascii
import os
from random import randint

import zmq

def socket_set_hwm(socket, hwm=-1):
    """libzmq 2/3/4 compatible sethwm"""
    try:
        socket.sndhwm = socket.rcvhwm = hwm
    except AttributeError:
        socket.hwm = hwm


def dump(msg_or_socket):
    """Receives all message parts from socket, printing each frame neatly"""
    if isinstance(msg_or_socket, zmq.Socket):
        # it's a socket, call on current message
        msg = msg_or_socket.recv_multipart()
    else:
        msg = msg_or_socket
    print("----------------------------------------")
    for part in msg:
        print("[%03d]" % len(part), end=' ')
        is_text = True
        try:
            print(part.decode('ascii'))
        except UnicodeDecodeError:
            print(r"0x%s" % (binascii.hexlify(part).decode('ascii')))


def set_id(zsocket, identity=None):
    """Set simple random printable identity on socket"""
    if identity is None:
        identity = u"%04x-%04x" % (randint(0, 0x10000), randint(0, 0x10000))
    zsocket.setsockopt_string(zmq.IDENTITY, identity)


def zpipe(ctx):
    """build inproc pipe for talking to threads

    mimic pipe used in czmq zthread_fork.

    Returns a pair of PAIRs connected via inproc
    """
    a = ctx.socket(zmq.PAIR)
    b = ctx.socket(zmq.PAIR)
    a.linger = b.linger = 0
    a.hwm = b.hwm = 1
    iface = "inproc://%s" % binascii.hexlify(os.urandom(8))
    a.bind(iface)
    b.connect(iface)
    return a,b

def zpipes(ctx, thread_num, addr):
    """build inproc pipe for talking to threads by router and dealer
    """
    a = ctx.socket(zmq.ROUTER)
    b = [ctx.socket(zmq.DEALER) for i in xrange(thread_num)]
    a.linger = 0

    iface = "inproc://%s" % addr
    a.bind(iface)

    for s in b:
        s.linger = 0
        s.connect(iface)

    return a, b

def poll(poller, ms):
    polls = dict(poller.poll(ms))
    ret = {}
    for sock, mask in polls.iteritems():
        oneret = []
        if mask & zmq.POLLIN:
            oneret.append(zmq.POLLIN)
        if mask & zmq.POLLOUT:
            oneret.append(zmq.POLLOUT)
        ret[sock] = oneret
    return ret
