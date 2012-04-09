#!/usr/bin/env python

##############################################################################
#
# $Id$
#
# file clerk set_cache_status currently can not send long messages
# this is a temporary solution
##############################################################################
import copy

import Trace

# @param fcc - file clerk client
# @param set_cache_params list of set_cache_status parameters to set
def set_cache_status(fcc, set_cache_params):
    
    Trace.trace(10, "set_cache_status: cache_params %s %s"%(len(set_cache_params),set_cache_params))
    # create a local copy of set_cache_params,
    # because the array will be modified by pop()
    local_set_cache_params = copy.copy(set_cache_params)
    list_of_set_cache_params = []
    tmp_list = []
    while len(local_set_cache_params) > 0:
        param = local_set_cache_params.pop()
        if param:
            if len(str(tmp_list)) + len(str(param)) < 15000: # the maximal message size is 16384
                tmp_list.append(param)
            else:
               Trace.trace(10, "set_cache_status appending %s"%(tmp_list,)) 
               list_of_set_cache_params.append(tmp_list)
               tmp_list = []
               tmp_list.append(param)
    if tmp_list:
        # append the rest
        list_of_set_cache_params.append(tmp_list)
    Trace.trace(10, "set_cache_status: params %s %s"%(len(list_of_set_cache_params), list_of_set_cache_params,))

    # now send all these parameters
    rc = None # define rc
    for param_list in list_of_set_cache_params:
        Trace.trace(10, "set_cache_status: sending set_cache_status %s %s"%(len(param_list), param_list,))

        rc = fcc.set_cache_status(param_list)
        Trace.trace(10, "set_cache_status: set_cache_status 1 returned %s"%(rc,))
    return rc
