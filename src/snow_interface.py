#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

import json
import os
import socket
import sys
import urllib2

from ConfigParser import ConfigParser

CONFIG_FILE = os.path.join(os.environ["ENSTORE_DIR"], "etc/servicenow_create_entry.cf")


def submit_ticket(**kwargs):
    config_parser = ConfigParser()
    config_parser.read(CONFIG_FILE)
    url = config_parser.get("HelpDesk", "incidentRestUrl")

    if not url:
        raise Exception("service now URL is not defined")

    password_manager = urllib2.HTTPPasswordMgrWithDefaultRealm()
    password_manager.add_password(None,
                                  url,
                                  config_parser.get("HelpDesk","acct"),
                                  config_parser.get("HelpDesk","passwd"))

    auth_handler = urllib2.HTTPBasicAuthHandler(password_manager)
    opener = urllib2.build_opener(auth_handler)
    urllib2.install_opener(opener)

    data = json.dumps({
        "impact":  kwargs.get("Impact_Type","3-Moderate/Limited"),
        "u_monitored_ci_name": kwargs.get("CiName").upper(),
        "short_description": kwargs.get("Summary", None),
        "description": kwargs.get("Notes", None),
        "u_reported_source": kwargs.get("Reported_Source_Type", "Event Monitoring"),
        "u_service": kwargs.get("Service_Type", "Storage"),
        "urgency":  kwargs.get("Urgency_Type", "3-Medium"),
        "u_monitored_categorization": kwargs.get("Product_Categorization_Tier_1",
                                                  config_parser.get("create_entry", "categorization")),
        "caller_id": config_parser.get("create_entry", "user_first") + " " +
                     config_parser.get("create_entry", "user_last"),
        "u_categorization": "1128451829dd90408638a6dc41528b56",
        "u_virtual_organization": "69f4000e6f4c9600c6df5d412e3ee43c",
        })

    request = urllib2.Request(url, data)
    request.add_header("Content-Type", "application/json")
    request.add_header("Accept", "application/json")

    try:
        response = urllib2.urlopen(request)
        if response.getcode() == 201:
            data = json.load(response)
            ticket = data["result"]["number"]
            return ticket
        else:
            raise Exception("Failed to create incident ticket HTTP %s" % (response.getcode()))
    except urllib2.HTTPError as e:
        raise Exception(str(e))

if __name__ == "__main__":
    try:
        ticket = submit_ticket(CiName=socket.gethostname().split(".")[0],
                               Summary="test summary",
                               Notes="test description")
        sys.stdout.write("Entry created with id= %s\n" % (ticket,))
        sys.exit(0)
    except Exception, msg:
        sys.stderr.write("%s\n" % (str(msg)))
        sys.exit(1)

