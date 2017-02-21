# -*- coding: utf-8 -*-

import re
import copy
import os
import errno


def makedir(diretory, mode = None):
    if not os.path.exists(diretory):
        try:
            os.makedirs(diretory)
        except OSError as e:
            if e.errno != errno.EEXIST:
                return str(e)
        if mode:
            os.chmod(diretory, mode)
    return None

ip_pattern = re.compile(r'^((2[0-4]\d|25[0-5]|[01]?\d\d?)\.){3}(2[0-4]\d|25[0-5]|[01]?\d\d?):|^localhost:')

class CommonPath(object):
    def __init__(self, path):
        self.path = copy.deepcopy(os.path.normpath(path))
        self.ip = ''
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
