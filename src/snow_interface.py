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

import ConfigParser

CONFIG_FILE = os.path.join(os.environ["ENSTORE_DIR"], "etc/servicenow_create_entry.cf")
HTTP_SUCCESS_CODES = (200, 201)

CI_NAME = socket.gethostname().split('.')[0].upper()
DESCRIPTION = "Please run lock on stkensrv4n.fnal.gov to write protect 10 tapes (2 caps)"
SHORT_DESCRIPTION = "write protect 10 tapes (flip tabs) in STKEN 8500GS tape library"


class SnowInterface(object):

    def __init__(self, config_file=None):
        """
        :param config_file: string, path to config file
        """
        self.config_file = config_file if config_file is None else CONFIG_FILE
        self.config_parser = ConfigParser.ConfigParser()
        self.config_parser.read(self.config_file)

        self.password_manager = urllib2.HTTPPasswordMgrWithDefaultRealm()
        auth_handler = urllib2.HTTPBasicAuthHandler(self.password_manager)
        opener = urllib2.build_opener(auth_handler)
        urllib2.install_opener(opener)

    def post(self, url, payload):
        """
        Creates/modifies entries in SNOW

        :param url: string, SNOW URL
        :param payload: dictionary of fields to be inserted
        :return: ticket number, string
        """
        self.password_manager.add_password(None,
                                           url,
                                           self.config_parser.get("HelpDesk", "acct"),
                                           self.config_parser.get("HelpDesk", "passwd"))
        request = urllib2.Request(url, json.dumps(payload))
        request.add_header("Content-Type", "application/json")
        request.add_header("Accept", "application/json")
        response = urllib2.urlopen(request)
        if response.getcode() not in HTTP_SUCCESS_CODES:
            raise Exception("HTTP code %s" % (response.getcode()))
        return json.load(response)

    def create_incident(self, **kwargs):
        """
        Creates incidents in SNOW

        :param kwargs: keyworded function arguments
        :return: ticket number, string
        """
        url = self.config_parser.get("HelpDesk", "incidentRestUrl")
        if not url:
            raise Exception("service now URL is not defined")

        data = {
            "impact":  kwargs.get("Impact_Type", "3-Moderate/Limited"),
            "u_monitored_ci_name": kwargs.get("CiName").upper(),
            "short_description": kwargs.get("Summary", None),
            "description": kwargs.get("Notes", None),
            "comments": kwargs.get("Comments", None),
            "u_reported_source": kwargs.get("Reported_Source_Type", "Event Monitoring"),
            "u_service": kwargs.get("Service_Type", "Storage"),
            "urgency":  kwargs.get("Urgency_Type", "3-Medium"),
            "u_monitored_categorization": kwargs.get("Monitored_Categorization",
                                                     self.config_parser.get("create_entry", "categorization")),
            "caller_id": self.config_parser.get("create_entry", "user_first") + " " +
                         self.config_parser.get("create_entry", "user_last"),
            "u_categorization": kwargs.get("Categorization",
                                           self.config_parser.get("create_entry", "u_categorization")),
            "u_virtual_organization": kwargs.get("Virtual_Organization",
                                                 self.config_parser.get("create_entry", "u_virtual_organization")),
            }
        response = self.post(url, data)
        return response["result"]["number"]

    def create_request(self, **kwargs):
        """
        Creates requests (RITMs) in SNOW

        :param kwargs: keyworded function arguments
        :return: ticket number, string
        """
        url = self.config_parser.get("HelpDesk", "requestRestUrl")
        if not url:
            raise Exception("service now URL is not defined")

        """
        First we create the request ticket
        """
        data = {
                "catalog_item": {
                    "sys_id": kwargs.get("Sys_Id",
                                         self.config_parser.get("create_entry", "sys_id")),
                    "vars": {
                        "u_monitored_ci_name": kwargs.get("CiName", CI_NAME).upper(),
                        "short_description": kwargs.get("Summary", SHORT_DESCRIPTION),
                        "description": kwargs.get("Description", DESCRIPTION),
                        "watch_list": kwargs.get("Watch_List",
                                                 self.config_parser.get("create_entry", "watch_list")),
                        "u_requestor_email": kwargs.get("E_Mail",
                                                        self.config_parser.get("create_entry", "u_requestor_email")),
                        }
                    }
                }
        response = self.post(url, data)
        ticket_number = response["items"][0]["number"]
        url = response["items"][0]["link"]

        """
        Then we update the ticket
        """
        update_data = {"u_categorization": self.config_parser.get("create_entry",
                                                                  "u_categorization"),
                       "priority": "3",
                       "u_virtual_organization": self.config_parser.get("create_entry",
                                                                        "u_virtual_organization"),
                       "urgency": "3",
                       "u_reported_source": self.config_parser.get("create_entry",
                                                                   "reported_source"),
                       "assignment_group": self.config_parser.get("create_entry",
                                                                  "assignment_group"),
                       }
        self.post(url, update_data)
        return ticket_number


def submit_ticket(**kwargs):
    """
    Creates incidents. For backward compatibility.
    :param kwargs: keyworded arguments
    :return: ticket number, string
    """
    snow = SnowInterface()
    return snow.create_incident(**kwargs)


if __name__ == "__main__":
    try:
        intf = SnowInterface()
        ticket = intf.create_incident(CiName=socket.gethostname().split(".")[0],
                                      Summary="test summary",
                                      Notes="test description")
        sys.stdout.write("Created incident with id= %s\n" % (ticket,))

        ticket = intf.create_request(CiName=socket.gethostname().split(".")[0],
                                     Summary="test summary",
                                     Notes="test description")
        sys.stdout.write("Created request with id= %s\n" % (ticket,))
        sys.exit(0)
    except Exception as e:
        sys.stderr.write("%s\n" % (str(e)))
        sys.exit(1)
