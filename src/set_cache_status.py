#!/usr/bin/env python

##############################################################################
#
# $Id$
#
# file clerk set_cache_status currently can not send long messages
# this is a temporary solution
##############################################################################

import Trace
import enstore_constants
import e_errors

PAD_SIZE = 200  # addtional pad for enstore message

# @param fcc - file clerk client
# @param set_cache_params list of set_cache_status parameters to set


def set_cache_status(fcc, set_cache_params):
    Trace.trace(10, "set_cache_status: cache_params %s %s" %
                (len(set_cache_params), set_cache_params))
    list_of_set_cache_params = []
    tmp_list = []
    for param in set_cache_params:
        if len(str(tmp_list)) + len(str(param)
                                    ) < enstore_constants.MAX_UDP_PACKET_SIZE - PAD_SIZE:
            tmp_list.append(param)
        else:
            Trace.trace(10, "set_cache_status appending %s" % (tmp_list,))
            list_of_set_cache_params.append(tmp_list)
            tmp_list = []
            tmp_list.append(param)
    if tmp_list:
        # append the rest
        list_of_set_cache_params.append(tmp_list)
    Trace.trace(10, "set_cache_status: params %s %s" %
                (len(list_of_set_cache_params), list_of_set_cache_params,))

    # now send all these parameters
    rc = None  # define rc
    for param_list in list_of_set_cache_params:
        Trace.trace(
            10,
            "set_cache_status: sending set_cache_status %s %s" %
            (len(
                str(param_list)),
                param_list,
             ))

        rc = fcc.set_cache_status(param_list, timeout=60, retry=2)
        Trace.trace(
            10, "set_cache_status: set_cache_status 1 returned %s" %
            (rc,))
        if not e_errors.is_ok(rc['status']):
            break
    return rc

# modify file records file records do not necessarily need to be complete
# @param fcc - file clerk client
# @param file_records list of file records


def modify_records(fcc, file_records):
    Trace.trace(
        10, "modify_records: file_records %s %s" %
        (len(file_records), file_records))
    list_of_records = []
    tmp_list = []

    for record in file_records:
        if len(str(tmp_list)) + len(str(record)
                                    ) < enstore_constants.MAX_UDP_PACKET_SIZE - PAD_SIZE:
            tmp_list.append(record)
        else:
            Trace.trace(
                10, "modify_records: list_of_records appending %s" %
                (tmp_list,))
            list_of_records.append(tmp_list)
            tmp_list = []
            tmp_list.append(record)
    if tmp_list:
        # append the rest
        list_of_records.append(tmp_list)
    Trace.trace(10, "modify_records: params %s %s" %
                (len(list_of_records), list_of_records,))

    # now send all these parameters
    rc = None  # define rc
    for record_list in list_of_records:
        Trace.trace(10, "modify_records: sending modify %s %s" %
                    (len(str(record_list)), record_list,))

        rc = fcc.modify(record_list, timeout=60, retry=2)
        Trace.trace(10, "modify_records: returned %s" % (rc,))
        if not e_errors.is_ok(rc['status']):
            break
    return rc
