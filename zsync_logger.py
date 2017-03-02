# -*- coding: utf-8 -*-
import locale
import logging
import ctypes
try:
    import curses
    HAVE_CURSES = True
except:
    HAVE_CURSES = False

MYLOGGER = logging.getLogger('MyLogger')
MYLOGGER.propagate = False

KSIZE = 1024
MSIZE = 1048576

def file_size_string(size):
    if size < KSIZE:
        return str(size)
    elif size < MSIZE:
        return '%.1fK' % (1.0 * size / KSIZE)
    else:
        return '%.1fM' % (1.0 * size / MSIZE)

class BaseLogger(logging.Handler):
    def __init__(self):
        logging.Handler.__init__(self)
        return

    def emit(self, record):
        raise NotImplementedError

    def _setCurPos(self):
        raise NotImplementedError

    def emit_file_progress(self, progress):
        raise NotImplementedError

class NoLogger(logging.Handler):
    def __init__(self):
        super(NoLogger, self).__init__()
        return

    def emit(self, record):
        return

    def emit_file_progress(self, progress):
        return 

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

class WinLogger(BaseLogger):
    def __init__(self):
        super(WinLogger, self).__init__()
        STD_OUTPUT_HANDLE= -11
        self.std_out_handle = ctypes.windll.kernel32.GetStdHandle(STD_OUTPUT_HANDLE)
        self.buffer_info = BufferInfo()
        self.curPos = COORD()
        self._updateCurPos()
        self.lastlines = 0
        return

    def _getCurPos(self):
        ctypes.windll.kernel32.GetConsoleScreenBufferInfo(self.std_out_handle, self.buffer_info)
        return self.buffer_info.dwCursorPosition.X, self.buffer_info.dwCursorPosition.Y

    def _updateCurPos(self):
        self.curPos.X, self.curPos.Y = self._getCurPos()
        return

    def _setCurPos(self):
        ctypes.windll.kernel32.SetConsoleCursorPosition(self.std_out_handle, self.curPos)
        return

    def emit(self, record):
        msg = self.format(record)
        print msg
        self._updateCurPos()
        return

    def emit_file_progress(self, progress):
        line_len = self.buffer_info.dwSize.X - 1
        lines = 0
        for file, curs, totals in progress:
            if totals < MSIZE:
                continue

            print file + ' ' * (line_len - len(file))

            string = '%s / %s' % (file_size_string(curs), file_size_string(totals))
            print string + ' ' * (line_len - len(string))
            lines += 2

        if self.lastlines > lines:
            string = ' ' * line_len
            for i in xrange(self.lastlines - lines):
                print string

        self.lastlines = lines
        self._setCurPos()
        return

class CursesLogger(BaseLogger):
    def __init__(self, screen):
        super(CursesLogger, self).__init__()
        self.screen = screen

        locale.setlocale(locale.LC_ALL, '') 
        self.locale_encoding = locale.getpreferredencoding()

        try:
            unicode
            self._unicode = True
        except:
            self._unicode = False

        self.curPos = 0, 0
        return

    def _updateCurPos(self):
        self.curPos = curses.getsyx()
        return

    def _setCurPos(self):
        curses.setsyx(*self.curPos)
        return

    def emit(self, record):
        try:
            msg = self.format(record)
            screen = self.screen
            fs = "\n%s"
            if not self._unicode: #if no unicode support...
                screen.addstr(fs % msg)
                screen.refresh()
            else:
                try:
                    if (isinstance(msg, unicode) ):
                        ufs = u'\n%s'
                        try:
                            screen.addstr(ufs % msg)
                            screen.refresh()
                        except UnicodeEncodeError:
                            screen.addstr((ufs % msg).encode(self.locale_encoding))
                            screen.refresh()
                    else:
                        screen.addstr(fs % msg)
                        screen.refresh()
                except UnicodeError:
                    screen.addstr(fs % msg.encode("UTF-8"))
                    screen.refresh()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

        self._updateCurPos()
        return

    def emit_file_progress(self, progress):
        return

def log_file_progress(progress):
    MYLOGGER.handlers[0].emit_file_progress(progress)
    return

def sys_excepthook(typ, value, tb):
    import pdb
    import traceback
    traceback.print_exception(typ, value, tb)
    msgs = []
    while tb:
        msg = 'locals: ' + str(tb.tb_frame.f_locals)
        if len(msg) > 2000:
            msg = msg[:2000] + ' Truncated...'
        msgs.append(msg)
        tb = tb.tb_next

    print '\n'.join(msgs)
    pdb.pm()
    return

def prepare_log(debug):
    import sys
    if debug:
        logging.basicConfig(level=logging.DEBUG, name='MyLogger')
        sys.excepthook = sys_excepthook
    else:
        logging.basicConfig(level=logging.INFO, name='MyLogger')
    return

def set_logger_handler(handler):
    global MYLOGGER
    formater = logging.Formatter(fmt='%(asctime)s.%(msecs)03d %(levelname)s: %(message)s',
            datefmt='%Y-%m-%d,%H:%M:%S')
    handler.setFormatter(formater)
    MYLOGGER.addHandler(handler)
    return

def curses_wrapper(stdscr, func, args):
    handler = CursesLogger(stdscr)
    set_logger_handler(handler)
    prepare_log(args.debug)
    func(args)
    return

def win_wrapper(func, args):
    handler = WinLogger()
    set_logger_handler(handler)
    prepare_log(args.debug)
    func(args)
    return

def nolog_wrapper(func, args):
    handler = NoLogger()
    set_logger_handler(handler)
    prepare_log(args.debug)
    func(args)
    return

def wrapper(func, args):
    if args.local:
        nolog_wrapper(func, args)
    elif HAVE_CURSES:
        curses.wrapper(func, args)
    else:
        win_wrapper(func, args)
    return