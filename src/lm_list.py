# library manager list of dictionaries
import os
import db
import Trace
import e_errors

class LMList:
    def __init__(self,db_dir=None,db_name=None, db_key=None):
	if db_dir and db_name:
	    # temporary fix: touch the file
	    f=open(os.path.join(db_dir, db_name)+".jou","w")
	    f.close()
	    self.dict = db.DbTable(db_name,db_dir, None, None, 0)
	    self.db_key = db_key
            self.db_name = db_name
	    self.have_db = 1
	else:
	    self.have_db = 0
	self.list = []
	

    # resotre list from DB
    def restore(self):
	if self.have_db:
	    self.dict.cursor("open")
	    key,value=self.dict.cursor("first")
	    while key:
		self.list.append(value)
                Trace.log(e_errors.INFO, "restoring LM list from DB %s:%s "%(self.db_name,repr(value)))
		key,value=self.dict.cursor("next")
	    self.dict.cursor("close")
	else:
	    pass

    # append to list
    def append(self, element, key=None):
	self.list.append(element)
	# create a record in the database
	if self.have_db:
	    # key is not specified
	    if (not key):
		# primary key is internally specified
		if self.db_key:
		    if element.has_key(self.db_key):
			key = element[self.db_key]
		    else: return
		else: return
	    self.dict[key] = element

    # remove from list
    def remove(self, element, key=None):
	self.list.remove(element)
	# remove a record from the database
	if self.have_db:
	    # key is not specified
	    if (not key):
		# primary key is internally specified
		if self.db_key:
		    if element.has_key(self.db_key):
			key = element[self.db_key]
		    else: return
		else: return
	    if self.dict.has_key(key):
		del self.dict[key]
	    




