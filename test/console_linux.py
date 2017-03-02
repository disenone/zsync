# -*- coding: utf-8 -*-
import time
import curses
import locale

locale.setlocale(locale.LC_ALL, '')
code = locale.getpreferredencoding()

def func(stdscr):
	# pad = curses.newpad(100, 100)
# #  These loops fill the pad with letters; this is
# # explained in the next section
	# for y in range(0, 100):
		# for x in range(0, 100):
			# try: pad.addch(y,x, ord('a') + (x*x+y*y) % 26 )
			# except curses.error: pass

# #  Displays a section of the pad in the middle of the screen
	# #pad.refresh( 0,0, 5,5, 20,75)
	stdscr.addstr(('%s' % '你好').decode('utf-8').encode(code))
	stdscr.refresh()
	time.sleep(5)
	y, x = stdscr.getyx()
	stdscr.move(y, 0)
	stdscr.addstr(('%s' % '傻逼').decode('utf-8').encode(code))
	stdscr.refresh()
	time.sleep(5)

curses.wrapper(func)

print code
# stdscr = curses.initscr()
# curses.noecho()
# curses.cbreak()
# stdscr.keypad(1)



# time.sleep(10)
# curses.nocbreak(); stdscr.keypad(0); curses.echo()
# curses.endwin()

