# -*- coding: utf-8 -*-

import re
import copy


ip_pattern = re.compile(r'^(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[0-9]{1,2})(\.(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[0-9]{1,2})){3}:')


class CommonFile(object):
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