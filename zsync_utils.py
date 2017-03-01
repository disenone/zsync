# -*- coding: utf-8 -*-

import re
import copy
import os
import stat
import errno
import config
import shutil
from zsync_logger import MYLOGGER
from collections import deque


def check_file_same(file_path, file_size, file_mtime):
    if not os.path.exists(file_path):
        return False
        
    file_stat = os.stat(file_path)
    
    if abs(file_stat.st_mtime - file_mtime) > config.FILE_MODIFY_WINDOW or \
        file_stat.st_size != file_size:
        return False
        
    return True

def fix_file_type(file_path, file_type, from_path=''):
    if os.path.exists(file_path):
        exist_type = config.FILE_TYPE_DIR if os.path.isdir(file_path) \
            else config.FILE_TYPE_FILE
            
        if exist_type != file_type:
            if exist_type == config.FILE_TYPE_FILE:
                os.remove(file_path)
            else:
                shutil.rmtree(file_path)
    else:
        if not file_path.startswith(from_path):
            raise ValueError('fix_file_type args is invalid')

        dirs = file_path[len(from_path):].split(os.path.sep)
        dirs = dirs[:-1]

        check_path = from_path

        for dir_part in dirs:
            check_path = os.path.join(check_path, dir_part)
            if os.path.exists(check_path) and not os.path.isdir(check_path):
                os.remove(check_path)
                return

    return

def makedir(directory, mode = None, mtime = None):
    if not os.path.exists(directory):
        try:
            os.makedirs(directory)
        except OSError as e:
            if e.errno != errno.EEXIST:
                return str(e)

    if mode or mtime:
        dir_stat = os.stat(directory)

        if mode and dir_stat[stat.ST_MODE] != mode:
            os.chmod(directory, mode)

        if mtime and dir_stat[stat.ST_MTIME] != mtime:
            os.utime(directory, (mtime, mtime))
    return None

ip_pattern = re.compile(r'^((2[0-4]\d|25[0-5]|[01]?\d\d?)\.){3}(2[0-4]\d|25[0-5]|[01]?\d\d?):|^localhost:')

class CommonPath(object):
    def __init__(self, path):
        self.origin_path = path
        self.ip = ''
        self.path = os.path.normpath(path)
        match = ip_pattern.match(self.path)
        if match:
            self.ip = match.group()[:-1]
            self.path = self.path[len(self.ip) + 1:]
        else:
            self.ip = '127.0.0.1'

        if self.ip == 'localhost':
            self.ip = '127.0.0.1'

        self.last_path = os.path.basename(self.path)
        self.prefix_path = self.path[:self.path.find(self.last_path)]
        return

    def isValid(self):
        return self.path and self.ip

    def full(self):
        return self.ip + ':' + self.path

    def isLocal(self):
        return self.ip == '127.0.0.1'

    def visitValid(self):
        if not os.path.exists(self.path):
            return False

        return True

class CommonFile(object):
    def __init__(self):
        self.path = ''
        self.file = None
        self.mode = 0
        self.file_mode = 0
        self.file_mtime = 0
        self.chunk_map = {}
        self.credit = 0
        self.fetch_offset = 0
        self.offset = 0
        self.total = 0
        self.writedone = False
        return

    def __del__(self):
        self.close()
        return

    def is_open(self):
        return self.file

    def writing_file(self):
        return self.file and 'w' in self.mode

    def close(self):
        if self.file:
            self.file.close()
            self.file = None
            if 'w' in self.mode:
                if self.file_mode:
                    os.chmod(self.path, self.file_mode)
                if self.file_mtime:
                    os.utime(self.path, (self.file_mtime, self.file_mtime))
        return

    def open(self, path, mode, total=0, credit=0, file_mode=0, file_mtime=0):
        self.close()
        self.__init__()
        self.path = path
        self.mode = mode
        self.file = open(path, mode)
        self.file_mode = file_mode
        self.file_mtime = file_mtime
        self.total = total
        self.credit = credit
        return

    def fetch(self, offset, size):
        self.offset = offset
        self.file.seek(offset, os.SEEK_SET)
        data = self.file.read(size)
        return data

    def write(self, data):
        if not self.file:
            return
        self.file.write(data)
        self.offset += len(data)
        if self.offset == self.total:
            self.writedone = True
            self.close()
        return

    def write_chunk(self, offset, data):
        if not self.file:
            return

        if offset != self.offset:
            self.chunk_map[offset] = data
        else:
            self.write(data)

            for woffset in sorted(self.chunk_map.keys()):
                if woffset == self.offset:
                    chunk = self.chunk_map.pop(woffset)
                    self.write(chunk)
                else:
                    break
        return


class CommonExclude(object):
    def __init__(self, excludes):
        self.excludes_origin = copy.deepcopy(excludes)
        self.excludes = None
        if excludes:
            self.excludes = [re.compile(exclude) for exclude in excludes]
        return

    # find match exclude
    # if fname is not none, will match fname and join(path, fname)
    # else match path
    def isExclude(self, path, fname=None):
        if not self.excludes:
            return False

        if fname:
            real_path = os.path.join(path, fname)
            for exclude in self.excludes:
                if exclude.match(fname):
                    return True

                if exclude.match(real_path):
                    return True
        else:
            for exclude in self.excludes:
                if exclude.match(path):
                    return True

        return False

def create_sub_process(args_dict):
    import subprocess
    import inspect
    import sys
    file_path = inspect.stack()[-1][1]
    dir_path = os.path.dirname(file_path)
    zsync_path = os.path.join(dir_path, 'zsync.py')

    for key in ['src', 'dst']:
        if type(args_dict[key]) is unicode:
            args_dict[key] = args_dict[key].encode(sys.stdin.encoding)

    sub_args = [
        'python', zsync_path,
        args_dict['src'], \
        args_dict['dst'], \
        '--port', str(args_dict['port']), \
        '--thread-num', str(args_dict['thread_num']), \
        '--timeout', str(args_dict['timeout']), \
        '--pipeline', str(args_dict['pipeline']), \
        '--chunksize', str(args_dict['chunksize']), \
    ]

    if args_dict['excludes']:
        for exclude in args_dict['excludes']:
            sub_args.extend(['--exclude', str(exclude)])

    if args_dict.get('local'):
        sub_args.append('--local')

    if args_dict.get('remote'):
        sub_args.append('--remote')

    if args_dict.get('compress'):
        sub_args.append('--compress')

    MYLOGGER.debug('creating subprocess %s' % sub_args)
    sub = subprocess.Popen(sub_args)
    return sub

CHECKSUM_M = 1 << 16

def calc_checksum(data):
    # a = sum(data) mod M
    # b = sum((i+1) * data[i]) mod M
    # s = a + 2^16 * b
    a = 0
    b = 0
    maxlen = len(data)
    for i, char in enumerate(data):
        ordc = ord(char)
        a += ordc
        b += (maxlen - i) * ordc
        if a > CHECKSUM_M:
            a %= CHECKSUM_M
        if b > CHECKSUM_M:
            b %= CHECKSUM_M
    s = a + (b << 16)
    return a, b, s

class RollingChecksum(object):
    def __init__(self, data=None):
        self.roll_queue = None
        self.a = 0
        self.b = 0
        self.s = 0
        if data:
            self.calc_checksum(data)
        return

    def calc_checksum(self, data):
        self.roll_queue = deque((ord(char) for char in data), len(data))
        maxlen = self.roll_queue.maxlen
        self.a, self.b, self.s = calc_checksum(data)
        return

    def calc_next(self, char):
        ordc = ord(char)
        self.a = (self.a - self.roll_queue[0] + ordc) % CHECKSUM_M
        self.b = (self.b - self.roll_queue.maxlen * self.roll_queue[0] + self.a) % CHECKSUM_M
        self.roll_queue.append(ordc)
        self.s = self.a + (self.b << 16)
        return
