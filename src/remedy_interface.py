#!/usr/bin/getenv python

###############################################################################
#
# $Id$
#
###############################################################################



import os
import sys
sys.path.insert(0,"/usr/local/etc")
import setups

import HelpDesk
import SubmitTicket
import ConfigParser

from HelpDesk import HelpDesk, HelpDeskException
from SubmitTicket import SubmitTicket
from ConfigParser import ConfigParser

def submit_ticket(**kwargs):
    #
    # The default values of variuos fields are chosen in such a way that
    # if executed w/o any arguments this function would create moderate urgency
    # ticket assigned to "Storage Service" group (that us "us")
    #
    config    = os.path.join(os.environ['ENSTORE_DIR'],'etc/create_entry.cf')
    help_desk = HelpDesk.create(config)
    config_parser = ConfigParser()
    config_parser.read(config)
    ciname = kwargs.get('CiName',None)
    notes  = kwargs.get('Notes',None)

    submitter = SubmitTicket(
        Last_Name      = config_parser.get('create_entry','JohnDoeLast','ENStore'),
        First_Name     = config_parser.get('create_entry','JohnDoeFirst','System'),
        Assigned_Group       = kwargs.get('Assigned_Group',config_parser.get('create_entry','assigntogroup','Storage Service')),
        Service_Type         = kwargs.get('Service_Type','Infrastructure Event'),
        Impact_Type          = kwargs.get('Impact_Type','3-Moderate/Limited'),
        Urgency_Type         = kwargs.get('Urgency_Type','3-Medium'),
        Reported_Source_Type = kwargs.get('Reported_Source_Type','Other'),
        Action               = kwargs.get('Action','CREATE'),
        Status_Type          = kwargs.get('Status_Type','New'),
        Summary              = kwargs.get('Summary',None))

    submitter.setProduct_Categorization(
        kwargs.get('Product_Categorization_Tier_1',config_parser.get('create_entry','Product_Categorization_Tier_1','Storage Services')),
        kwargs.get('Product_Categorization_Tier_2',config_parser.get('create_entry','Product_Categorization_Tier_2','Enstore')),
        config_parser.get('create_entry','Product_Categorization_Tier_3',None))
    submitter.setCIName(ciname)
    submitter.setNotes(notes)
    ticket = submitter.submit()
    return ticket


if __name__ == "__main__":
    ticket=submit_ticket()
    print "Entry created with id=", ticket
