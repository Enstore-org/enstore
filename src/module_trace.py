#!/usr/bin/env python
from __future__ import print_function
import sys
import string
import pprint
import os

mtable = {}

rm_table = {}


def get_module_file(m):
    for i in sys.path:
        p = os.path.join(i, m + '.py')
        if os.access(p, os.R_OK):
            return p
        p = os.path.join(i, m + '.so')
        if os.access(p, os.R_OK):
            return p
        p = os.path.join(i, m + 'module.c')
        if os.access(p, os.R_OK):
            return p
    return None


def mtrace(m):
    if m in mtable:
        return
    mtable[m] = []
    mf = get_module_file(m)
    if mf is None:
        return
    f = open(mf)
    l = f.readline()
    while l:
        idx = string.find(l, '"""')
        if idx != -1 and idx == string.rfind(l, '"""'):
            l = f.readline()
            while l and string.find(l, '"""') == -1:
                l = f.readline()
        if not l:
            break
        l = string.strip(l)
        l = string.replace(l, ',', ' ')
        token = string.split(l)
        tl = len(token)
        i = 0
        while i < tl:
            if token[i][0] == '#':  # comment
                break
            if (token[i] == 'from' or token[i] == '"from') \
                    and i + 2 < tl and token[i + 2] == 'import':
                module = token[i + 1]
                if not module in mtable[m]:
                    mtable[m].append(module)
                mtrace(module)
                i = i + 2
            elif token[i] == 'import':
                for j in token[i + 1:]:
                    if j[0] == '#':  # comment
                        break
                    module = j
                    if module[-1] == ',':
                        module = module[:-1]
                    if not module in mtable[m]:
                        mtable[m].append(module)
                    mtrace(module)
                    i = i + 1
            i = i + 1
        l = f.readline()


counter = 0


def log_trace(t):
    if t[-1] not in rm_table or len(t) < len(rm_table[t[-1]]):
        rm_table[t[-1]] = t


def trace_path(history, module):
    global counter
    counter = counter + 1
    # print counter, `history`, module
    # if counter > 100:
    #	return
    if module in history:
        log_trace(history + [module])
        return

    if module in mtable:
        a = history + [module]
        for i in mtable[module]:
            trace_path(a, i)
        log_trace(a)


if __name__ == '__main__':
    m = string.split(sys.argv[1], '.')[0]
    mtrace(m)
    # pprint.pprint(mtable)
    trace_path([], m)
    kl = 0
    for k in rm_table.keys():
        if len(k) > kl:
            kl = len(k)

    output = sorted(rm_table.values())
    for i in output:
        cs = '%-' + repr(kl) + 's :'
        print(cs % (i[-1]), end=' ')
        for j in i[:-1]:
            print(j, '->', end=' ')
        print(i[-1])
