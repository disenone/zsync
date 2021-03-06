# -*- coding: utf-8 -*-

import ctypes
import logging

class COORD(ctypes.Structure):
    _fields_ = [("X", ctypes.c_short), ("Y", ctypes.c_short)]

class SMALL_RECT(ctypes.Structure):
    _fields_ = [
        ('Left', ctypes.c_short),
        ('Top', ctypes.c_short),
        ('Right', ctypes.c_short),
        ('Bottom', ctypes.c_short),
    ]

class BufferInfo(ctypes.Structure):
    _fields_ = [
        ('dwSize', COORD),
        ('dwCursorPosition', COORD),
        ('wAttributes', ctypes.c_ushort),
        ('srWindow', SMALL_RECT),
        ('dwMaximumWindowSize', COORD),
    ]

def win_console():
    STD_OUTPUT_HANDLE= -11
    std_out_handle = ctypes.windll.kernel32.GetStdHandle(STD_OUTPUT_HANDLE)
    buffer_info = BufferInfo()
    ctypes.windll.kernel32.GetConsoleScreenBufferInfo(std_out_handle, buffer_info)
    print buffer_info.dwSize.X, buffer_info.dwSize.Y, buffer_info.dwCursorPosition.X, buffer_info.dwCursorPosition.Y
    logging.error('test')
    ctypes.windll.kernel32.GetConsoleScreenBufferInfo(std_out_handle, buffer_info)
    print buffer_info.dwSize.X, buffer_info.dwSize.Y, buffer_info.dwCursorPosition.X, buffer_info.dwCursorPosition.Y
    return
    dwCursorPosition = COORD()
    dwCursorPosition.X = buffer_info.dwCursorPosition.X
    dwCursorPosition.Y = buffer_info.dwCursorPosition.Y+10
    ctypes.windll.kernel32.SetConsoleCursorPosition(std_out_handle,dwCursorPosition)
    i=1
    while True:
        logging.error(i)
        logging.error(i + 1)
        if i == 1:
            print 'hihi'
        i += 1
        ctypes.windll.kernel32.SetConsoleCursorPosition(std_out_handle,dwCursorPosition)
    exit()

def write_return():
    logging.basicConfig(format=u'%(asctime)s.%(msecs)03d %(levelname)s: %(message)s',
            datefmt='%Y-%m-%d,%H:%M:%S', level=logging.DEBUG)

    logging.error(u'你好')
    return

if __name__ == '__main__':
    win_console()
    #write_return()
