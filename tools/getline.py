# $Id$

import string
filecache = {}


def fixindent(line):
    level=0
    rest=line
    while line:
        c, line = line[0], line[1:]
        if c not in ' \t':
            #went too far, put the char back
            line=c+line
            break
        if c==' ':
            level=level+1
        elif c=='\t':
            #take it out to the next multiple of 8
            level=((level+8)/8)*8
    line=level*' '+line
    return line


    

def getline(fname,lno):
    if fname not in filecache.keys():
        try:
            text =  open(fname,'r').readlines()
            text = map(string.rstrip, text)
            text = map(fixindent, text)
            filecache[fname] = text
        except:
            return None
    lno = lno-1
    lines = filecache[fname]
    if lno<0 or lno>=len(lines):
        return None
    return lines[lno]
