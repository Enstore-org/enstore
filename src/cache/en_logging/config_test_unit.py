#!/usr/bin/env python

##############################################################################
#
# $Id$
##############################################################################
import logging
import string

# enstore imports
#import Trace
#import log_client

def set_logging_console(name=None,full_name=None):
    # configure two logging channeles to report to console. One logging channel is Trace, the other is Log
    # both channels can be configured to report to enstore's Trace.log and Trace.trace 
    
    lh = logging.StreamHandler()
    #    fmt = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    # %(pathname)s 
#    fmt = logging.Formatter("%(filename)s %(lineno)d :: %(name)s :: %(module)s :: %(levelname)s :: %(message)s")
    fmt = logging.Formatter("%(levelname)s chan=%(name)s module=%(module)s file=%(filename)s %(lineno)d %(message)s")
    
    l_log = logging.getLogger('log.encache')
    l_trace = logging.getLogger('trace.encache')
    
    #add formatter to lh
    lh.setFormatter(fmt)
    l_log.addHandler(lh)
    l_trace.addHandler(lh)
    
    l_log.setLevel(logging.DEBUG)
    l_trace.setLevel(logging.DEBUG)

    return (l_log, l_trace)

#def set_logging_enstore(name=None,conf_srv=None,full_name=None):
#    # initialize enstore Trace if it is used for logging 
#    # 
#    Trace.init(string.upper(name))
#    if conf_srv :
#        logc = log_client.LoggerClient(conf_srv, name)
#    
#    # not complete
