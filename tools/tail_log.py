#!/usr/bin/env python

import sys, os

if len(sys.argv)<2:
    print "Usage: %s filename [maxlines] [chunksize]" % (sys.argv[0])
    sys.exit(-1)
filename = sys.argv[1]
maxlines = 1000
if len(sys.argv)>2:
    maxlines = int(sys.argv[2])

chunksize = maxlines/10
if len(sys.argv)>3:
    chunksize = int(sys.argv[3])

outfile = open(filename, 'w')

def lose_head(filename, n):
    f = open(filename, 'r')
    lines = f.readlines()
    f.close()
    lines = lines[n:]
    tmp = filename+'.tmp'
    f = open(tmp, 'w')
    f.writelines(lines)
    f.close()
    os.rename(tmp, filename)
    f = open(filename, 'a')
    return f, len(lines)

nlines = 0
while 1:
    line = sys.stdin.readline()
    if not line:
        break
    sys.stdout.write(line)
    outfile.write(line)
    sys.stdout.flush()
    outfile.flush()
    nlines = nlines + 1
    if nlines >= maxlines:
        outfile.close()
        outfile, nlines = lose_head(filename, chunksize)
        
        
