#!/usr/bin/env python

"""
This script creates requests tickets in ServiceNow
to be used to create tab flip tickets
"""

import os
import time
from ConfigParser import ConfigParser
from optparse import OptionParser
import socket

import suds.client


CONFIG_FILE=os.path.join(os.environ["ENSTORE_DIR"],"etc/servicenow_create_entry.cf")

""" ServiceNow development URL used to insert requests """
DEV_SNOW_PUT_URL="https://fermidev.service-now.com/CreateRequestedItem.do?WSDL"
""" ServiceNow production URL used to insert requests """
PRD_SNOW_PUT_URL="https://fermi.service-now.com/CreateRequestedItem.do?WSDL"
""" ServiceNow development URL used to retrieve created ticket """
DEV_SNOW_GET_URL="https://fermidev.service-now.com/sc_req_item.do?WSDL"
""" ServiceNow production URL used to retrieve created ticket """
PRD_SNOW_GET_URL="https://fermi.service-now.com/sc_req_item.do?WSDL"
""" Assignment group """
ASSIGNMENT_GROUP="Logistics/PREP Support"
""" Magic categorization item that initiates the workflow """
CAT_ITEM="18c27227042950008638553dd6544037"
CALLER_ID="Dms-enstore System"
AFFILIATION="Fermilab"
E_MAIL="ssa-auto@fnal.gov"
DESCRIPTION="Please run lock on stkensrv4n.fnal.gov to write protect 10 tapes (2 caps)"
IMPACT = "3 - Moderate/Limited"
INCIDENT_STATE="New"
OPENED_BY="Enstore System"
PRIORITY="3-Medium"
SEVERITY=""
SHORT_DESCRIPTION="write protect 10 tapes (flip tabs) in STKEN 8500GS tape library"
COMMENTS="Please run lock on stkensrv4n.fnal.gov to write protect 10 tapes (2 caps)"

MONITORED_CATEGORIZATION="Scientific Data Storage and Access -- Enstore Tape Storage"
REPORTED_SOURCE="Event Monitoring"
SERVICE="Infrastructure Event"
CI_NAME=socket.gethostname().split('.')[0].upper()
URGENCY="3 - Medium"

def submit_ticket(**kwargs):
    config_parser = ConfigParser()
    config_parser.read(CONFIG_FILE)
    url = DEV_SNOW_PUT_URL if kwargs.get("Dev") else PRD_SNOW_PUT_URL
    client = suds.client.Client(url,
                                username=config_parser.get("HelpDesk","acct","cd-srv-dms-enstore"),
                                password=config_parser.get("HelpDesk","passwd"))
    method = client.service.execute
    result = method(assignment_group           = kwargs.get("Assignment_Group", ASSIGNMENT_GROUP),
                    cat_item                   = kwargs.get("Cat_Item",CAT_ITEM),
                    caller_id                  = kwargs.get("Caller_Id",CALLER_ID),
                    affiliation                = kwargs.get("Affiliation",AFFILIATION),
                    email                      = kwargs.get("E_mail",E_MAIL),
                    description                = kwargs.get("Description",DESCRIPTION),
                    impact                     = kwargs.get("Impact",IMPACT),
                    incident_state             = kwargs.get("Incident_State",INCIDENT_STATE),
                    opened_by                  = kwargs.get("Opened_By",OPENED_BY),
                    opened_at                  = time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime()),
                    priority                   = kwargs.get("Priority",PRIORITY),
                    severity                   = kwargs.get("Severity",SEVERITY),
                    short_description          = kwargs.get("Summary",SHORT_DESCRIPTION),
                    comments                   = kwargs.get("Comments",COMMENTS),
                    u_monitored_categorization = kwargs.get("Monitored_Categorization",MONITORED_CATEGORIZATION),
                    u_reported_source          = kwargs.get("Reported_Source",REPORTED_SOURCE),
                    u_service                  = kwargs.get("Service",SERVICE),
                    u_monitored_ci_name        = kwargs.get("CiName",CI_NAME).upper(),
                    urgency                    = kwargs.get("Urgency",URGENCY)
                    )
    url = DEV_SNOW_GET_URL if kwargs.get("Dev") else PRD_SNOW_GET_URL
    client = suds.client.Client(url,
                                username=config_parser.get("HelpDesk","acct","cd-srv-dms-enstore"),
                                password=config_parser.get("HelpDesk","passwd"))
    result = client.service.get(sys_id=result)
    return result.number

def help():
    return "usage %prog [options]"

if __name__ == "__main__":

    parser = OptionParser(usage=help())
    parser.add_option("-p", "--prd",action="store_true",
                      dest="prd",default=False,
                      help="Create ticket in production system [default: %default] ")

    (options, args) = parser.parse_args()

    if options.prd:
        result=submit_ticket()
    else:
        result=submit_ticket(Dev=True)
    print result

