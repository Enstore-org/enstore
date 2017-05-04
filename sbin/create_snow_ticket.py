#!/usr/bin/env python

import sys
import snow_interface

if __name__ == '__main__':
    try:
        ticket = snow_interface.submit_ticket(
            CiName  = sys.argv[1].upper(),
            Summary = sys.argv[1].upper() + " : " + sys.argv[3][:100],
            Notes   = sys.argv[4],
            Impact_Type  = '2-Significant/Large',
            Urgency_Type = '1-Critical',
            Assigned_Group = 'Storage Service',
            Action = 'CREATE',
            Status_Type = 'Assigned',
            Assigned_Support_Company = 'Fermilab',
            Assigned_Support_Organization = 'Computing Division')
        sys.stdout.write("Entry created with id= %s\n"%(ticket,))
        sys.exit(0)
    except Exception, msg:
        sys.stderr.write("%s\n"%(str(msg)))
        sys.exit(1)

