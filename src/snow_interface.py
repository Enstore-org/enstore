#!/usr/bin/getenv python

###############################################################################
#
# $Id$
#
###############################################################################

import json
import os
import sys
import urllib2

from ConfigParser import ConfigParser

CONFIG_FILE=os.path.join(os.environ["ENSTORE_DIR"],"etc/servicenow_create_entry.cf")

def submit_ticket(**kwargs):

    config_parser = ConfigParser()
    config_parser.read(CONFIG_FILE)
    url = config_parser.get("HelpDesk","incidenRestUrl")

    if not url :
        raise Exception("service now URL is not defined")

    password_manager = urllib2.HTTPPasswordMgrWithDefaultRealm()
    password_manager.add_password(None,
                                  url,
                                  config_parser.get("HelpDesk","acct"),
                                  config_parser.get("HelpDesk","passwd"))
    data = json.dumps({
        'impact' :  kwargs.get('Impact_Type','3-Moderate/Limited'),
        'u_virtual_organization' : config_parser.get('create_entry','virtual_organization','Other'),
        'u_monitored_ci_name' : kwargs.get('CiName').upper(),
        'short_description' : kwargs.get('Summary',None),
        'description' : kwargs.get('Notes',None),
        'u_reported_source value' : kwargs.get('Reported_Source_Type','Event Monitoring'),
        'u_service value' : kwargs.get('Service_Type','Storage'),
        'urgency' :  kwargs.get('Urgency_Type','3-Medium'),
        'u_monitored_categorization' : kwargs.get('Product_Categorization_Tier_1',
                                                  config_parser.get('create_entry','categorization')),
        'u_categorization' :  kwargs.get('Product_Categorization_Tier_1',
                                         config_parser.get('create_entry','categorization')),
        })

    request = urllib2.Request(url,data)
    request.add_header("Content-Type","application/json")
    request.add_header("Accept","application/json")

    try:
        response = urllib2.urlopen(request)
        if response.getcode() == 201:
            data = json.load(response)
            ticket = data['result']['number']
            return ticket
        else:
            raise Exception("Failed to create incident ticket HTTP %s"%(response.getcode()))
    except urllib2.HTTPError as e:
        raise Exception(str(e))

if __name__ == "__main__":
    try:
        ticket=submit_ticket()
        sys.stdout.write("Entry created with id= %s\n"%(ticket,))
        sys.exit(0)
    except Exception, msg:
        sys.stderr.write("%s\n"%(str(msg)))
        sys.exit(1)

