# $Id$
    
filecache = {}

def getline(fname,lno):
    if fname not in filecache.keys():
        try:
            filecache[fname] = open(fname,'r').readlines()
        except:
            return None
    lno = lno-1
    lines = filecache[fname]
    if lno<0 or lno>=len(lines):
        return None
    return lines[lno][:-1]
