###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import os
import time
import copy

# enstore imports
import log_client
import journal
import table
import Trace

import libtpshelve

JOURNAL_LIMIT=1000
backup_flag=1
cursor_open=0

class MyIndex(table.Index):
  def __init__(self,db,name):
	table.Index.__init__(self,db,name)
  def val_to_str(self,val):
	if val==None:
		return None
	return val
  def str_to_val(self,str):
	if str==None:
		return None
	return str

class DbTable:
  def __init__(self,dbname,logc,indlst=[]):
    try:
	dbHome=os.environ['ENSTORE_DB']
    except:
	dbHome=os.environ['ENSTORE_DIR']
    dbEnvSet={'create':1,'init_mpool':1, 'init_lock':1, 'init_txn':1}
    dbEnv=libtpshelve.env(dbHome,dbEnvSet)
    self.db=libtpshelve.open(dbEnv,dbname,type='btree')
    self.dbindex=libtpshelve.open(dbEnv,"index",type='btree')
    self.inx={}
    for name in indlst:
    	self.inx[name]=MyIndex(self.dbindex,name)
    self.jou=journal.JournalDict({},dbname+".jou")
    self.count=0
    self.name=dbname
    self.logc=logc
    if len(self.jou):
    	self.start_backup()
	self.checkpoint()
	self.stop_backup()
  def next(self):
    if cursor_open==0:
	return self.cursor("open")
    return self.cursor("next")
  def cursor(self,action,key="None",value=0):
    global c
    global t
    global cursor_open
    if action=="open":
       if cursor_open==0:
          t=self.db.txn()
          c=self.db.cursor(t)
          key,value=c.first()
          cursor_open=1
          return key
    if action=="close":
       if cursor_open:
          c.close()
          t.commit()
          cursor_open=0
          return 0
    if action=="len":
       if cursor_open:
          pos,value=c.get()
          len=0
          last,value=c.last()
          key,value=c.first()
          while key!=last:
             key,val=c.next()
             len=len+1
             c.set(pos)
          return len
    if action=="has_key":
       if cursor_open:
          pos,value=c.get()
          key,value=c.set(key)
          c.set(pos)
          if key:
		return 1
          else:
                return 0

    if action=="delete":
        pass

    if action=="get":
        key,value=c.set(key)
        return value
    if action=="update":
        c.set(key)
        c.update(value)
        return key
    if action=="next":
        key,value=c.next()
        if key:
           pass
        else:
           self.cursor("close")
        return key


  def keys(self):
    return self.db.keys()
  def __len__(self):
    if cursor_open==1:
        return self.cursor("len")
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
     if 'db_flag' in value.keys(): del value['db_flag']
     self.jou[key]=copy.deepcopy(value)
     self.jou[key]['db_flag']='add'
     self.count=self.count+1
     if self.count > JOURNAL_LIMIT and backup_flag:
           self.checkpoint()
     for name in self.inx.keys():
        self.inx[name][value[name]]=key
     if cursor_open==1:
           self.cursor("update",key,value)
	   return
     t=self.db.txn()
     self.db[(key,t)]=value
     t.commit()

  def is_index(self,key):
	if self.inx.has_key(key):
		return 1
	return 0
  def index(self,field,field_val):
       try:
	return self.inx[field][field_val]
       except:
	return []

  def __getitem__(self,key):
     if cursor_open==1:
     	return self.cursor("get",key)
     return self.db[key]

  def __delitem__(self,key):
     value=self.db[key]
     if self.jou.has_key(key) == 0:
      	self.jou[key]=copy.deepcopy(self.db[key])
     else:
	if self.jou[key]['db_flag']=='delete':
		return
     self.jou[key]['db_flag']='delete'
     del self.jou[key]
     t=self.db.txn()
     del self.db[(key,t)]
     t.commit()
     self.count=self.count+1
     if self.count > JOURNAL_LIMIT and backup_flag:
      	self.checkpoint()
     for name in self.inx.keys():
        del self.inx[name][(key,value[name])]
  def dump(self):
     t=self.db.txn()
     c=self.db.cursor(t)
     print c.first()
     while c:
	 print c.next()
     c.close()
     t.commit()
  def close(self):
     self.jou.close()
     if cursor_open==1:
	self.cursor("close")
     self.db.close()
  def checkpoint(self):
     import regex,string
     import time
     del self.jou
     self.logc.send(log_client.INFO, 1, "Start checkpoint for "+self.name+" journal")
     cmd="mv " + os.environ['ENSTORE_DB'] +"/"+self.name+".jou " + \
                        os.environ['ENSTORE_DB'] +"/"+self.name+".jou."+ \
                        repr(time.time())
     os.system(cmd)
     self.jou = journal.JournalDict({},self.name+".jou")
     self.count=0
     self.logc.send(log_client.INFO, 1, "End checkpoint for "+self.name)
  def start_backup(self):
     global  backup_flag
     backup_flag=0
     self.logc.send(log_client.INFO, 1, "Start backup for "+self.name)
     self.checkpoint()
  def stop_backup(self):
     global  backup_flag
     backup_flag=1
     self.logc.send(log_client.INFO, 1, "End backup for "+self.name)
def do_backup(name):
     import time
     try:
    	   import SOCKS; socket = SOCKS
     except ImportError:
    	   import socket

     try:
	   dir_bck=os.environ['ENSTORE_DB_BACKUP']
     except:
	   dir_bck="/tmp/backup"
     try:
	   hst_bck=os.environ['ENSTORE_BCKP_HST']
     except:
	   hst_bck=socket.gethostname()
     cwd=os.getcwd()
     os.chdir(os.environ['ENSTORE_DB'])
     cmd="tar cvf "+name+".tar "+name+" "+name+".jou.*"
     print cmd
     os.system(cmd)
     cmd="rm "+name +".jou.*"
     print cmd
     os.system(cmd)
     os.chdir(cwd)
if __name__=="__main__":
  import sys
  Trace.init("dbclerk")
  Trace.trace(1,"dbc called with args "+repr(sys.argv))

  dict= DbTable(sys.argv[1],0)
  dict.dump()
  Trace.trace(1,"dbc exit ok")
