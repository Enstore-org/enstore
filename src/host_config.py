#!/usr/bin/env python
#
# $Id$
#

"""The client-side configuration file will be called /etc/enstore.conf but this can
be overridden with env. var. ENSTORE_CONF"""

import os
import sys
import string

def find_config_file():
    filename = os.environ.get("ENSTORE_CONF", "/etc/enstore.conf")
    if os.path.exists(filename):
        return filename
    return None

def read_config_file(filename):
    if not filename:
        return None
    config = {}
    try:
        f = open(filename, 'r')
    except:
        sys.stderr.write("Can't open %s" % (filename,))
        return None
    for line in f.readlines():
        comment = string.find(line, "#")
        if comment >= 0:
            line = line[:comment]
        line = string.strip(line)
        if not line:
            continue
        tokens = string.split(line)
        ntokens = len(tokens)
        first = 1
        for token in tokens:
            eq = string.find(token,'=')
            if eq<=0:
                sys.stderr.write("%s: syntax error, %s"%(filename, token))
                f.close()
                return None
            key, value = token[:eq], token[eq+1:]
            try:
                value=int(value)
            except ValueError:
                try:
                    value = float(value)
                except:
                    pass
            if first:
                first = 0
                if ntokens == 1:
                    config[key] = value
                else:
                    config[key] = config.get(key,{})
                    subdict = {}
                    config[key][value]=subdict
            else:
                subdict[key]=value
    f.close()
    return config

_cached_config = None

def get_config():
    global _cached_config
    if not _cached_config:
        _cached_config = read_config_file(find_config_file())
    return _cached_config

        
