#!/usr/bin/env python 
"""Executable. Given module, prints all imported modules and import chains."""
import sys
import string
import pprint
import os

mtable = {}

rm_table = {}

def get_module_file(m):
    """Takes module name, returns path to the implementation file."""
    for i in sys.path:
        p = os.path.join(i, m+'.py')
        if os.access(p, os.R_OK):
            return p
        p = os.path.join(i, m+'.so')
        if os.access(p, os.R_OK):
            return p
        p = os.path.join(i, m+'module.c')
        if os.access(p, os.R_OK):
            return p
    return None

def mtrace(m):
    """Takes module name, adds imported modules recursively to mtable."""
    if mtable.has_key(m):
        return
    mtable[m] = []
    mf = get_module_file(m)
    if mf == None:
        return
    f = open(mf)
    l = f.readline()
    while l:
        # Skipping doc strings at beginning of file
        idx = string.find(l, '"""')
        if idx != -1 and idx == string.rfind(l, '"""'):
            l = f.readline()
            while l and string.find(l, '"""') == -1:
                l = f.readline()
        # If there are no more lines, exit
        if not l:
            break
        # Sanitize line and split into tokens
        l = string.strip(l)
        l = string.replace(l, ',', ' ')
        token = string.split(l)
        tl = len(token)
        i = 0
        # Iterate over tokens
        while i < tl:
            # Skip to next line if we hit a commnet
            if token[i][0] == '#': # comment
                break
            # If line is `from <mod> import *`
            if (token[i] == 'from' or token[i] == '"from') \
                and i +2 < tl and token[i+2] == 'import':
                module = token[i+1]
                # Add mod to mtable and recurse
                if not module in mtable[m]:
                    mtable[m].append(module)
                mtrace(module)
                # Move past 'from <mod>'
                i = i + 2
            # If line is `import <mod>[,<mod>]*`
            elif token[i] == 'import':
                # Iterate over further tokens
                for j in token[i+1:]:
                    # Exit if we hit a commnet
                    if j[0] == '#': # comment
                        break
                    module = j
                    if module[-1] == ',':
                        module = module[:-1]
                    # Add mod to mtable and recurse
                    if not module in mtable[m]:
                        mtable[m].append(module)
                    mtrace(module)
                    # Move to next mod
                    i = i + 1
            # Go to next token
            i = i + 1
        # Go to next line
        l = f.readline()

counter = 0

def log_trace(t):
    """Stores the impot path (t) to a module (t[-1]) rm_table[t[-1]]"""
    if not rm_table.has_key(t[-1]) or len(t) < len(rm_table[t[-1]]):
        rm_table[t[-1]] = t

def trace_path(history, module):
    """Explore module tree imported by a module.

    Given a module, explores the module tree imported by that module according to mtable, and adds the route to each module to rm_table.
  
    Args:
    module: The name of the module who's import tree should be explored.
      This module should already be in mtable.
    history: Set of modules which should be included 'above' the module passed,
      i.a. its ancestors.
    """
    global counter
    counter = counter + 1
    # print counter, `history`, module
    # if counter > 100:
    #  return
    if module in history:
        log_trace(history+[module])
        return

    if mtable.has_key(module):
        a = history + [module]
        for i in mtable[module]:
            trace_path(a, i)
        log_trace(a)

   
if __name__ == "__main__":
    m = string.split(sys.argv[1], '.')[0]
    mtrace(m)
    # pprint.pprint(mtable)
    trace_path([], m)
    kl = 0
    for k in rm_table.keys():
        if len(k) > kl:
            kl = len(k)

    output = rm_table.values()
    output.sort()
    for i in output:
        cs = '%-'+`kl`+'s :'
        print cs%(i[-1]),
        for j in i[:-1]:
            print j, '->',
        print i[-1]
