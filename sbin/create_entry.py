#!/usr/bin/env python
import sys
sys.path.insert(0,"/usr/local/etc")
import setups

ups = setups.setups()
ups.use_python("v2_4_5")

from HelpDesk import HelpDesk, HelpDeskException
from SubmitTicket import SubmitTicket
from ConfigParser import ConfigParser
import os
import sys

class CreateEntry:
    def __init__(self, cfg, argv):
        self.cfp = ConfigParser()
        self.cfp.read(cfg)
        uselast = self.cfp.get('create_entry','JohnDoeLast','')
        usefirst = self.cfp.get('create_entry','JohnDoeFirst','')
        usegroup = self.cfp.get('create_entry','AssignToGroup',None)

        ciname = argv[1]
        condition = argv[2]
        summary = argv[3][:100]
        notes = argv[4]
        #subuser = argv[5]  # no longer used
        #subpass = argv[6]    # no longer used
        if len(argv) > 6:
	    cat1 = argv[7]   # old PTI
	    cat2 = argv[8]
	    cat3 = argv[9]
        else:
            cat1 =  self.cfp.get('create_entry','Categorization_Tier_1',None)
            cat2 =  self.cfp.get('create_entry','Categorization_Tier_2',None)
            cat3 = self.cfp.get('create_entry','Categorization_Tier_3',None)

        submitter = SubmitTicket(
				 Last_Name = uselast,
				First_Name = usefirst,
			      Service_Type = 'Infrastructure Event',
			       Impact_Type = '2-Significant/Large',
			      Urgency_Type = '1-Critical',
				   Summary = summary,
		      Reported_Source_Type = 'Other',
			    Assigned_Group = usegroup,
				    Action = 'CREATE',
			       Status_Type = 'Assigned',
		  Assigned_Support_Company = 'Fermilab',
	     Assigned_Support_Organization = 'Computing Division',
 	)
 	submitter.setCIName(ciname)
 	submitter.setNotes(notes)
        submitter.setCategorization( cat1, cat2, cat3 )
        submitter.setProduct_Categorization(
             self.cfp.get('create_entry','Product_Categorization_Tier_1','Storage Services'),
             self.cfp.get('create_entry','Product_Categorization_Tier_2','Enstore'),
             self.cfp.get('create_entry','Product_Categorization_Tier_3',None))
 	#print "Submitter looks like:" , submitter.__dict__
 	ticket = submitter.submit()
        print "Entry created with id=", ticket

if __name__ == '__main__':
    cfg = os.environ['ENSTORE_DIR'] + '/etc/create_entry.cf'
    helpDesk = HelpDesk.create(cfg)
    CreateEntry(cfg,sys.argv)
