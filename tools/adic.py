#!/usr/bin/env python

import os
import string
import re
import time

log = os.popen("./adic.exp", 'r')

cmd_dict = {}
while 1:
    line = log.readline()
    tok = string.split(line)
    if len(tok) < 6:
        continue
    
    dat, tim, ampm, junk, seq = tok[:5]
    rest = tok[5:]

    if rest[0]=='Command':
        cmd_dict[seq] = rest[2:]
    
    if rest[0] in ['Positive', 'Negative']:
        cmd = cmd_dict.get(seq)
        if not cmd:
            continue
        print string.join(cmd,' '), rest[0]
        del cmd_dict[seq]
    

""" Lines look like this.  The time is always wrong

09-28-00 12:44:01 pm 0 0827     Command 0827: MONT of Volser ..........PRF022 to D22.......<01030>

"""

