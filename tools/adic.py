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
    
    dat, tim, seq, junk = tok[:4]
    rest = tok[4:]

    if len(rest)>2 and rest[2]=='command:':
        cmd_dict[seq] = (rest[3:],dat,tim)
    
    if rest[0] in ['Positive', 'Negative']:
        (cmd,dat0,tim0) = cmd_dict.get(seq)
        if not cmd:
            continue
        if dat == dat0:
            day="%s %s->%s"%(dat,tim0,tim)
        else:
            day="%s %s->%s %s"%(dat0,tim0,dat,tim)
        print string.join(cmd,' '), rest[0], day
        del cmd_dict[seq]
    

""" Lines look like this. (All times are now correct.)

09-28-00 12:44:01 pm 0 0827     Command 0827: MONT of Volser ..........PRF022 to D22.......<01030>

2000-10-18 17:06:35     0687 <01030> RQM requests command: KEEP from Drive D25........
2000-10-18 17:06:35     0687 <01056> KEEP [D25.......] received, will be processed soon.
2000-10-18 17:06:35     0687 <01094> The requested drive DC25010101 is empty (Archive catalog).
2000-10-18 17:06:35     0687 <01041> Negative answer: KEEP 0687  RC: 1094.

"""

