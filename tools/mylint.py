#!/usr/bin/env python

import sys, os
import string
import re

gadfly_dir=os.environ.get('GADFLY_GRAMMAR')
if not gadfly_dir:
    print "Error, environment variable GADFLY_GRAMMAR must be set"
    print "Try 'setup gadfly'"
    sys.exit(-1)

#hack gadfly dir into the path, this way we don't have to mess with PYTHON_PATH    
sys.path[:1] = [gadfly_dir]


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

import kjpylint

#initialize
exit_status = 1

#collect errors in a list rather than printing them
error_list=[]

def complain(error):
    error_list.append(error)


ignore_patterns = [".*defined before [0-9]+ not used$",
                   "Warning: set of global .* in local context",
                   ]

ignore_list = map(re.compile, ignore_patterns)

verbose=0
args = sys.argv[1:]

if args and args[0]=='-v':
    verbose=1
    args=args[1:]

lno_at_beginning=re.compile("^[0-9]+:")
lno_anywhere=re.compile("at ([0-9]+) ")

for filename in args:
    (pyg, context) = kjpylint.setup()
    context.complain=complain
    exit_status = 0
    error_list=[]
    parse_errors=[]
    data = open(filename,'r').read()
    data = data+'\n\n'
    lines=string.split(data,'\n')
    lines=map(fixindent,lines)
    data =string.join(lines,'\n')
    print "=============="
    print "Checking", filename
    try:
        kjpylint.lint(data,pyg,context)
    except:
        exc,msg,tb=sys.exc_info()
        parse_errors.append("%s %s"%(exc, string.join(map(str,msg.args[:-1]))))
    if parse_errors:
        exit_status = 1
        print "Fatal errors occurred during parsing of", filename
        for err in parse_errors:
            print " ", err
        print "Please resolve these and repeat check"
        continue
    for err in error_list:
        for pattern in ignore_list:
            if pattern.match(err):
                if verbose:
                    print "ignoring", err
                break
        else:
            exit_status=1
            if not lno_at_beginning.match(err):
                lno = lno_anywhere.search(err)
                if lno:
                    err=lno.group(1)+":"+err
            print filename+":"+err
            
print "Exiting with %s"%(exit_status,)
sys.exit(exit_status)
    

