#!/usr/bin/env python

# $Id$            
            
import os, sys
import re, string

def preprocess(lines):
    """ remove all comments and triple-quoted strings from a list of lines"""
    copy, comment, q1, q2, q3, q2a, q1a = range(7)
    inbuf = string.join(lines,'')
    outbuf = ''
    q=""
    state = copy
    for c in inbuf:
        if state==copy:
            if c=='#':
                state=comment
            elif c in ("'", '"'):
                state=q1
                q=c
            else:
                outbuf=outbuf+c
        elif state==comment:
            if c=='\n':
                outbuf=outbuf+c
                state=copy
        elif state==q1:
            if c==q:
                state=q2
            else:
                outbuf=outbuf+q
                outbuf=outbuf+c
                state=copy
        elif state==q2:
            if c==q:
                state=q3
            else:
                outbuf=outbuf+q
                outbuf=outbuf+q
                outbuf=outbuf+c
                state=copy
        elif state==q3:
            if c==q:
                state=q2a
        elif state==q2a:
            if c==q:
                state=q1a
            else:
                state=q3
        elif state==q1a:
            if c==q:
                state=copy
            else:
                state=q3
    return string.split(outbuf,'\n')

def find_imports(filename, imports):
    key = os.path.splitext(os.path.basename(filename))[0]
    if key not in imports.keys():
        imports[key] = []
    l = imports[key]
    lines = open(filename, 'r').readlines()
    lines = preprocess(lines)
    for line in lines:
        words = re.split("[, \t]*", string.strip(line))
        if not words:
            continue
        if words[0]=="import":
            for w in words[1:]:
                if w not in l:
                    l.append(w)
            l.extend(words[1:])
        elif words[0]=="from":
            if words[1] not in l:
                l.append(words[1])

warned=[]

def check(top, chain, imports, sub=None):
##    print "check", top, chain, sub
    if sub==top:
        return 1
    if sub is None:
        sub=top
    if sub in chain:
        return 0
    chain.append(sub)
    for mod in imports[sub]:
        if mod not in imports.keys():
            if mod not in warned:
                print "\tDon't know imports of", mod
            warned.append(mod)
        else:
            if check(top, chain, imports, mod):
                return 1
    chain.pop()
    return 0
        
    
def find_circ(name, imports):
    chain = []
    if check(name, chain, imports):
        return chain
    else:
        return None

            


if __name__=="__main__":

    if len(sys.argv)<2:
        print "Usage: %s file-or-dir [file-or-dir...]" % sys.argv[0]
        sys.exit(-1)
    files = []
    for arg in sys.argv[1:]:
        if not os.path.exists(arg):
            print arg+": no such file or directory"
            continue
        if os.path.isdir(arg):
            for f in os.listdir(arg):
                base, ext = os.path.splitext(f)
                if ext != ".py":
                    continue
                files.append(os.path.join(arg, f))
        else:
            files.append(arg)

    if not files:
        sys.exit(0)
    print "Finding imports"
    imports = {}
    for f in files:
        find_imports(f, imports)

    keys = imports.keys()
    keys.sort()

    for key in keys:
        print key, "imports", string.join(imports[key])
        
    print "\nScanning for circularities"
    ok = 1
    
    for key in keys:
        print "Checking", key
        circ = find_circ(key, imports)
        if circ:
            ok = 0
            print key, "has circular dependencies:\n\t",
            for c in circ:
                print c+" ->",
            print circ[0]
        
            
