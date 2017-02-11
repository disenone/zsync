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


ip_pattern = re.compile(r'^(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[0-9]{1,2})(\.(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[0-9]{1,2})){3}:')

class CommonPath(object):
    def __init__(self, path):
        self.path = copy.deepcopy(path)
        self.ip = ''
        match = ip_pattern.match(self.path)
        if match:
            self.path = self.path[len(self.ip):]
            self.ip = match.group()[:-1]
        else:
            self.ip = 'localhost'
        return

    def isValid(self):
        return self.path and self.ip

    def full(self):
        return self.ip + ':' + self.path

class CommonFile(object):
    def __init__(self):
        self.path = ''
        self.file = None
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
        return

    def open(self, path, mode, total=0, credit=0):
        self.close()
        self.__init__()
        self.path = path
        self.file = open(path, mode)
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
