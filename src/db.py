###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import os
import time

# enstore imports
import journal
import Trace
import interface
import configuration_client
import e_errors

import libtpshelve

JOURNAL_LIMIT=1000
backup_flag=1
cursor_open=0

class DbTable:
  def __init__(self,dbname, db_home, jou_home, indlst=None, auto_journal=1):
    if indlst is None:
        indlst = []
    self.auto_journal = auto_journal
    self.dbHome = db_home
    self.jouHome = jou_home

#    try:
#	self.dbHome=configuration_client.ConfigurationClient(\
#		(interface.default_host(),\
#		interface.default_port())).get('database')['db_dir']
#    except:
#	self.dbHome=os.environ['ENSTORE_DIR']
    dbEnvSet={'create':1,'init_mpool':1, 'init_lock':1, 'init_txn':1}
    dbEnv=libtpshelve.env(self.dbHome,dbEnvSet)
    self.db=libtpshelve.open(dbEnv,dbname,type='btree')
#junk     self.dbindex=libtpshelve.open(dbEnv,"index",type='btree')
#junk     self.inx={}

#junk     for name in indlst:
#junk     	self.inx[name]=MyIndex(self.dbindex,name)

    if self.auto_journal:
        self.jou=journal.JournalDict({},self.jouHome+"/"+dbname+".jou", 1)
        self.count=0

    self.name=dbname
    if self.auto_journal:
      if len(self.jou):
        self.start_backup()
        self.checkpoint()
        self.stop_backup()

  #def next(self):
  #  return self.cursor("next")

  def cursor(self,action,KeyOrValue=None):
    global c
    global t
    global cursor_open
    if not cursor_open and action !="open":
      self.cursor("open")

    if action=="open":
      t=self.db.txn()
      c=self.db.cursor(t)
      cursor_open=1
      return

    if action=="close":
      c.close()
      t.commit()
      cursor_open=0
      return

    if action=="first":
      return c.first()

    if action=="last":
      return c.last()

    if action=="next":
      return c.next()

    # this should really be replaced by the db_stat command
    if action=="len":
      pos,value=c.get()
      len=0
      last,value=c.last()
      key,value=c.first()
      while key!=last:
        key,val=c.next()
        len=len+1
        c.set(pos)
      return len+1

    if action=="has_key":
      pos,value=c.get()
      key,value=c.set(KeyOrValue)
      c.set(pos)
      if key:
        return 1
      else:
        return 0

    if action=="delete":
      if self.auto_journal:
        if self.jou.has_key(c.Key) == 0:
          self.jou[c.Key]=self.db[c.Key]  ## was deepcopy
        else:
          if self.jou[c.Key]['db_flag']=='delete':
            return
        self.jou[c.Key]['db_flag']='delete'
        del self.jou[c.Key]
      return c.delete()
   
    if action=="get":
      return c.set(KeyOrValue)

    if action=="update":
      if self.auto_journal:
        if 'db_flag' in KeyOrValue.keys(): del KeyOrValue['db_flag']
        self.jou[c.Key]=KeyOrValue  ## was deepcopy
        self.count=self.count+1
        if self.count > JOURNAL_LIMIT and backup_flag:
          self.checkpoint()

      status = c.update(KeyOrValue)

      if self.auto_journal:
        self.jou[c.Key]['db_flag']='add'

      return status

  def keys(self):
    return self.db.keys()

  def __len__(self):
    t=self.db.txn()
    c=self.db.cursor(t)
    last,val=c.last()
    key,val=c.first()
    len=0
    while key!=last:
	key,val=c.next()
	len=len+1
    c.close()
    t.commit()
    return len

  def has_key(self,key):
     return self.db.has_key(key)

  def __setitem__(self,key,value):
     if self.auto_journal:
       if 'db_flag' in value.keys(): del value['db_flag']
       self.jou[key]=value  ## was deepcopy
       self.count=self.count+1
       if self.count > JOURNAL_LIMIT and backup_flag:
           self.checkpoint()

#junk      for name in self.inx.keys():
#junk         self.inx[name][value[name]]=key

     t=self.db.txn()
     self.db[(key,t)]=value
     t.commit()

     if self.auto_journal:
       self.jou[key]['db_flag']='add'

#junk   def is_index(self,key):
#junk         if self.inx.has_key(key):
#junk 		return 1
#junk 	return 0

#junk   def index(self,field,field_val):
#junk        try:
#junk 	return self.inx[field][field_val]
#junk        except:
#junk 	return []

  def __getitem__(self,key):
     return self.db[key]

  def __delitem__(self,key):
     value=self.db[key]

     if self.auto_journal:
       if self.jou.has_key(key) == 0:
         self.jou[key]=self.db[key]  ## was deepcopy
       else:
         if self.jou[key]['db_flag']=='delete':
		return
       self.jou[key]['db_flag']='delete'
       del self.jou[key]

     t=self.db.txn()
     del self.db[(key,t)]
     t.commit()
     if self.auto_journal:
       self.count=self.count+1
       if self.count > JOURNAL_LIMIT and backup_flag:
      	 self.checkpoint()
#junk      for name in self.inx.keys():
#junk         del self.inx[name][(key,value[name])]

  def dump(self):
     t=self.db.txn()
     c=self.db.cursor(t)
     Trace.log(e_errors.INFO,repr(c.first()))
     while c:
#	No! You don't want to dump the whole database into Trace.log ...
#	 Trace.log(e_errors.INFO,c.next())
         print c.next()
     c.close()
     t.commit()

  def close(self):
     if self.auto_journal:
        self.jou.close()
     if cursor_open==1:
	self.cursor("close")
     self.db.close()

  def checkpoint(self):
     #import regex,string
     if self.auto_journal:
        del self.jou
     Trace.log(e_errors.INFO, "Start checkpoint for "+self.name+" journal")
     cmd="mv " + self.jouHome +"/"+self.name+".jou " + \
                        self.jouHome +"/"+self.name+".jou."+ \
                        repr(time.time())
     os.system(cmd)
     if self.auto_journal:
        self.jou = journal.JournalDict({},self.jouHome+"/"+self.name+".jou", 1)
     self.count=0
     Trace.log(e_errors.INFO, "End checkpoint for "+self.name)
  def start_backup(self):
     global  backup_flag
     backup_flag=0
     Trace.log(e_errors.INFO, "Start backup for "+self.name)
     self.checkpoint()
  def stop_backup(self):
     global  backup_flag
     backup_flag=1
     Trace.log(e_errors.INFO, "End backup for "+self.name)

  # backup is a method of DbTable
  def backup(self):
     cwd=os.getcwd()
     os.chdir(self.dbHome)
     cmd="tar cf "+self.name+".tar "+self.name
     Trace.log(e_errors.INFO, repr(cmd))
     os.system(cmd)
     os.chdir(self.jouHome)
     cmd="tar rf "+self.dbHome+"/"+self.name+".tar"+" "+self.name+".jou.*"
     Trace.log(e_errors.INFO, repr(cmd))
     os.system(cmd)
     cmd="rm "+ self.name +".jou.*"
     Trace.log(e_errors.INFO, repr(cmd))
     os.system(cmd)
     os.chdir(cwd)
			
  # cross_check() cross check journal dictionary and database

  def cross_check(self):

        error = 0

        # check if the items in db has the same value of that
        # in journal dictionary

        for i in self.dict.keys():
            if not self.has_key(i):
                print 'M> key('+i+') is not in database'
                error = error + 1
            elif `self.dict[i]` != `self.__getitem__(i)`:
                print 'C> database and journal disagree on key('+i+')'
                print 'C>  journal['+i+'] =', self.dict[i]
                print 'C> database['+i+'] =', self.__getitem__(i)
                error = error + 1
#        # check if the deleted items are still in db
#
#        for i in self.deletes:
#            if self.has_key(i):
#                print 'D> database['+i+'] should be deleted'
#                error = error + 1

        return error


def do_backup(name, dbHome, jouHome):
     cwd=os.getcwd()
#     try:
#         dbHome = configuration_client.ConfigurationClient(\
#		(interface.default_host(),\
#		interface.default_port()), 3).get('database')['db_dir']
#
#     except:
#         dbHome = os.environ['ENSTORE_DIR']
     os.chdir(dbHome)
     cmd="tar cf "+name+".tar "+name
     Trace.log(e_errors.INFO, repr(cmd))
     os.system(cmd)
     os.chdir(jouHome)
     cmd="tar rf "+dbHome+"/"+name+".tar"+" "+name+".jou.*"
     Trace.log(e_errors.INFO, repr(cmd))
     os.system(cmd)
     cmd="rm "+name +".jou.*"
     Trace.log(e_errors.INFO, repr(cmd))
     os.system(cmd)
     os.chdir(cwd)
if __name__=="__main__":
  import sys
  Trace.init("DBCLERK")
  Trace.trace(6,"dbc called with args "+repr(sys.argv))

  dict= DbTable(sys.argv[1],0)
  dict.dump()
  Trace.trace(6,"dbc exit ok")
