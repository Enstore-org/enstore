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

2000-10-18 17:23:33     6789 <01296> DAS4060 Mount request from client d0ensrv4 - volser PRF025, drive DC22.
2000-10-18 17:23:33     0796 <01030> RQM requests command: MONT of Volser ..........PRF025 to D22........
2000-10-18 17:23:33   000000 <01150> <**** RQMA010796QCARY2T102300101DC22010101YD...........PRF025 
2000-10-18 17:23:42   000000 <01077> ****> RQMA010796S0000..........PRF025 
2000-10-18 17:23:42     0796 <01041> Positive answer: MONT 0796    .
2000-10-18 17:23:42     6789 <01296> DAS4061 Mount request from client d0ensrv4 completed successfully.

2000-10-18 17:28:11     6810 <01296> DAS4070 Keep request from client d0ensrv4 - volser , drive DC20.
2000-10-18 17:28:11     0817 <01030> RQM requests command: KEEP from Drive D20........
2000-10-18 17:28:11     0817 <01056> KEEP [D20.......] received, will be processed soon.
2000-10-18 17:28:11   000000 <01150> <**** RQMA010817QCARY2DC20010101T103090507ND...........PRF126 
2000-10-18 17:28:22   000000 <01077> ****> RQMA010817S0000................ 
2000-10-18 17:28:22     0817 <01041> Positive answer: KEEP 0817    .
2000-10-18 17:28:22     6810 <01296> DAS4071 Keep request from client d0ensrv4 completed successfully.


"""

