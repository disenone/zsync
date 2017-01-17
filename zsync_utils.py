# -*- coding: utf-8 -*-

import re

ip_pattern = re.compile(r'(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[0-9]{1,2})(\.(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[0-9]{1,2})){3}:')

class CommonFile(object):
    def __init__(self, str):
        self.path = str
        self.isIP = ip_pattern.match(self.path) is not None
        return
