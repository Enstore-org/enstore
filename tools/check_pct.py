#!/usr/bin/env python

# $Id$
import sys
import string
import types

import parser
from token import *
from symbol import *
import pprint

debug=0

def factor_type(lst):
    if lst[0] != factor:
        raise TypeError, lst[0]
    if lst[1][0] != power:
        if lst[1][1] not in '+-~':
            raise parser.ParserError
        return factor_type(lst[2])
    else:
        return lst[1][1][1]

def get_item(lst,indices):
    ret=lst
    for i in indices:
        ret=ret[i]
    return ret

def count_percent(s):
    s=string.replace(s,'%%','')
    count=0
    for c in s:
        if c=='%':
            count=count+1
    return count

def is_terminal(lst):
    return type(lst)==types.ListType and len(lst)==3 and map(type,lst)==[
        types.IntType, types.StringType, types.IntType]
    
def get_terminals(lst,indices=()):
    ret=[]
    sub = get_item(lst,indices)
    if is_terminal(sub):
        ret.append(indices,sub[2])
    else:
        for i in range(1,len(sub)):
            ret.extend(get_terminals(lst,indices+(i,)))
    return ret

def flatten(lst,ret=None):
    if not ret:
        ret=[]
    for x in lst:
        if type(x)==type([]):
            flatten(x,ret)
        else:
            ret.append(x)
    return ret

def get_code(filename):
    file=open(filename)
    code=file.read()
    file.close()
    return code
    
def get_ast(filename):
    code=get_code(filename)
    ast = parser.suite(code)
    return ast.tolist(1)

def get_codelines(filename):
    code=get_code(filename)
    return ['']+string.split(code,'\n')
    
def check_pct(filename, warn=0):

    ast=get_ast(filename)
    codelines=get_codelines(filename)

    terms = get_terminals(ast)

    for idx, line in terms:
        line=line
        item=get_item(ast,idx)
        if item[0]==PERCENT:
            pos = idx[-1]
            pre_pos= idx[:-1]+(pos-1,)
            op_pos= idx
            post_pos= idx[:-1]+(pos+1,)
            pre=get_item(ast,pre_pos)
            op=get_item(ast,op_pos)
            post=get_item(ast,post_pos)

            if factor_type(pre)[0] != STRING:
                continue

            tok_type = factor_type(post)[0]
            if tok_type == LPAR:
                xx=post_pos
                lst=get_item(ast,xx+(1,1,2))[1:]
                if debug: print lst
                narg=0
                comma=0
                for x in lst:
                    if x[0]==COMMA:
                        comma=1
                    else:
                        narg=narg+1
                if not comma:
                    narg=1
            elif tok_type in [NUMBER,STRING,LSQB,LBRACE]:
                narg=1
            elif tok_type == NAME:
                narg = -1  #not a sequence type
            else:
                print "Unexpected token", factor_type(post)

            fmt_string=pre[1][1][1][1]
            npct = count_percent(fmt_string)

            ##XXX Todo:  check for format string constructions like %(key)d
            # These cannot be mixed with other types.  Check that the RHS is a dict ?
            
            if narg>=0 and npct!=narg or narg==-1 and npct!=1:
                print "%s:%d: format string %s has %d %% signs, arg tuple has length %d" % \
                      (filename,line,fmt_string, npct,narg)
            elif warn and npct==1 and narg==-1:
                print "%s:%d: format string %s: possible mismatch (arg may be a tuple)" %\
                      (filename,line,fmt_string)

if __name__ == "__main__":
    args=sys.argv[1:]
    warn = 0
    if not args:
        print "Usage: %s [-w] file.py [...]" % sys.argv[0]
        sys.exit(-1)
    if args[0] == '-w':
        warn=1
        args=args[1:]
    for fname in args:
#        try:
            check_pct(fname,warn)
#        except:
#            print "Fatal error checking", fname
            








