#!/usr/bin/env python

"""
This script creates requests tickets in ServiceNow
to be used to create tab flip tickets
"""

import socket
import snow_interface


SHORT_DESCRIPTION = "write protect 10 tapes (flip tabs) in STKEN 8500GS tape library"
COMMENTS = "Please run lock on stkensrv4n.fnal.gov to write protect 10 tapes (2 caps)"
DESCRIPTION = "Please run lock on stkensrv4n.fnal.gov to write protect 10 tapes (2 caps)"
CI_NAME = socket.gethostname().split('.')[0].upper()


def submit_ticket(**kwargs):
    intf = snow_interface.SnowInterface()
    ticket = intf.create_request(CiName=kwargs.get("CiName", CI_NAME).upper(),
                                 Summary=kwargs.get("Summary", SHORT_DESCRIPTION),
                                 Notes=kwargs.get("Description", DESCRIPTION),
                                 Comments=kwargs.get("Comments", COMMENTS),)
    return ticket

if __name__ == "__main__":
    result = submit_ticket()
    print(result)
