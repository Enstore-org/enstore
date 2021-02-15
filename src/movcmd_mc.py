#!/usr/bin/env python

# $Id$

from __future__ import print_function
import os
import string
import sys

from en_eval import en_eval


def endswith(s1, s2):
    return s1[-len(s2):] == s2


_config_cache = None


def dict_eval(data):
    # This is like "eval" but it assumes the input is a
    # dictionary; any trailing junk will be ignored.
    last_brace = string.rindex(data, '}')
    try:
        d = en_eval(data[:last_brace + 1])
    except BaseException:
        print("Error", data, end=' ')
        d = {}
    return d


def get_config():
    global _config_cache
    if _config_cache:
        return _config_cache
    p = os.popen("enstore config --show", 'r')
    _config_cache = dict_eval(p.read())
    p.close()
    return _config_cache


def get_movers(config=None):
    movers = []
    if not config:
        config = get_config()
    for item, value in config.items():
        if endswith(item, '.mover'):
            mover = item[:-6]
            movers.append(mover)
        movers.sort()
    return movers


def get_media_changer(mover, config=None):
    mc = 'NotFound'
    if not config:
        config = get_config()
    for item, value in config.items():
        if item == mover:
            mc = value.get('media_changer', 'Unknown.media_changer')
            mc = mc[:-14]
            break
    return mc


def mc_for_movers():
    movers = get_movers()
    mc = {}
    for mover in movers:
        med = get_media_changer(mover + '.mover')
        if med in mc:
            mc_l = mc[med]
            mc_l.append(mover)
            mc[med] = mc_l
        else:
            mc[med] = [mover, ]
    return mc


if __name__ == "__main__":

    try:
        med_cha_match = sys.argv[1]
    except BaseException:
        med_cha_match = 'aml'
    try:
        command = sys.argv[2]
    except BaseException:
        command = 'status'
    try:
        skip_movers = sys.argv[3:]
    except BaseException:
        skip_movers = []

    med_cha = mc_for_movers()

    for mc, movers in med_cha.items():
        if string.find(mc, med_cha_match) >= 0:
            for mover in movers:
                cmd = "enstore mov --%s %s" % (command, mover)
                skipit = 0
                for skip in skip_movers:
                    if string.find(mover, skip) >= 0:
                        skipit = 1
                        break
                if skipit == 0:
                    print(cmd)
                    p = os.popen(cmd, 'r')
                    response = p.read()
                    p.close()
                    print(response)
                else:
                    print('skipping', cmd)
