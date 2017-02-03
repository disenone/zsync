# -*- coding: utf-8 -*-

import signal
import sys
 
def signal_term_handler(signal, frame):
    print 'got SIGTERM'
    sys.exit(0)
 
signal.signal(signal.SIGTERM, signal_term_handler)
signal.signal(signal.SIGABRT, signal_term_handler)
signal.signal(signal.SIGBREAK, signal_term_handler)

while True:
    pass