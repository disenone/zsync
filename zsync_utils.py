# -*- coding: utf-8 -*-

import re
import copy
import os
import stat
import errno
import config
import shutil

def check_file_same(file_path, file_size, file_time):
    if not os.path.exists(file_path):
        return False
        
    file_stat = os.stat(file_path)
    
    if file_stat[stat.ST_TIME] != file_time:
        return False
    
    if file_stat[stat.ST_SIZE] != file_size:
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

def makedir(directory, mode = None):
    if not os.path.exists(directory):
        try:
            os.makedirs(directory)
        except OSError as e:
            if e.errno != errno.EEXIST:
                return str(e)
     if mode:
        if os.stat(directory)[stat.ST_MODE] != mode:
            os.chmod(diretory, mode)
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
        self.chunk_map = {}
        self.credit = 0
        self.fetch_offset = 0
        self.write_offset = 0
        self.total = 0
        self.writedone = False
        return

    def __del__(self):
        self.close()
        return

    def close(self):
        if self.file:
            self.file.close()
            self.file = None
            if 'w' in self.mode and self.file_mode:
                os.chmod(self.path, self.file_mode)
        return

    def open(self, path, mode, total=0, credit=0, file_mode=0):
        self.close()
        self.__init__()
        self.path = path
        self.mode = mode
        self.file = open(path, mode)
        self.file_mode = file_mode
        self.total = total
        self.credit = credit
        return

    def fetch(self, offset, size):
        self.file.seek(offset, os.SEEK_SET)
        data = self.file.read(size)
        return data

    def write(self, data):
        if self.writedone:
            return
        self.file.write(data)
        self.write_offset += len(data)
        if self.write_offset == self.total:
            self.writedone = True
            self.close()
        return

    def write_chunk(self, offset, data):
        if self.writedone:
            return

        if offset != self.write_offset:
            self.chunk_map[offset] = data
        else:
            self.write(data)

            for woffset in sorted(self.chunk_map.keys()):
                if woffset == self.write_offset:
                    chunk = self.chunk_map.pop(woffset)
                    self.write(chunk)
                else:
                    break
        return


class CommonExclude(object):
    def __init__(self, excludes):
        self.excludes_origin = copy.deepcopy(excludes)
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
