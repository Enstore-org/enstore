import shelve
import os
import time
import copy
import log_client
from  journal import JournalDict

JOURNAL_LIMIT=1000
backup_flag=1

class dBTable:
	def __init__(self,dbname,logc):
		self.db=shelve.open( os.environ['ENSTORE_DB']+"/"+dbname)
	        self.jou=JournalDict({},dbname+".jou")
                self.count=0
                self.name=dbname
		self.logc=logc
		if len(self.jou) :
			self.start_backup()
			self.checkpoint()
			self.stop_backup()
        def keys(self):
                keylist=[]
                for key in self.db.keys():
                   if self.jou.has_key(key):
                      if self.jou[key]['db_flag']=='delete':
                         continue
                   keylist.append(key)
                for key in self.jou.keys():
                      if self.jou[key]['db_flag']=='add':
                         keylist.append(key)
                return keylist
        def __len__(self):
                num_del=0
                num_add=0
                for key in self.jou.keys():
                    if self.jou[key]['db_flag']=='add': num_add=num_add+1
                    if self.jou[key]['db_flag']=='delete': num_del=num_del+1   
                length=len(self.db)+num_add-num_del
                return length
        def has_key(self,key):
                if self.jou.has_key(key):
		    if self.jou[key]['db_flag'] !='delete':
                        return self.jou.has_key(key)
		    else :
			return 0
                return self.db.has_key(key)
        def __setitem__(self,key,value) :
                if 'db_flag' in value.keys(): del value['db_flag']
                self.jou[key]=value               
                self.jou[key]['db_flag']='add'
                self.count=self.count+1
                if self.count > JOURNAL_LIMIT and backup_flag :
                       self.checkpoint()
        def __getitem__(self,key):
               if self.jou.has_key(key) == 0:
                   self.jou[key]=copy.deepcopy(self.db[key])
                   self.jou[key]['db_flag']='0'
                   self.count=self.count+1
                   if self.count > JOURNAL_LIMIT and backup_flag :
                       self.checkpoint()
		       return self.db[key]
               return self.jou[key]
        def __delitem__(self,key):
              if self.jou.has_key(key) == 0:
                   self.jou[key]=copy.deepcopy(self.db[key])
	      else :
		if self.jou[key]['db_flag']=='delete':
			return      
              self.jou[key]['db_flag']='delete'
              del self.jou[key]
	      self.count=self.count+1
              if self.count > JOURNAL_LIMIT and backup_flag :
                   self.checkpoint()

	def dump(self):
	      for key in self.db.keys():
	           print key,"  -  ",self.db[key]
              for key in self.jou.keys():
                   print key,"  -  ",self.jou[key]
	def close(self):
              self.jou.close()
	      self.db.close()
        def add(self,key,value):
                tmp=copy.deepcopy(value)
                if 'db_flag' in tmp.keys():
                        del tmp['db_flag']
                self.db[key]=tmp
        def delete(self,key):
                del self.db[key]


        def checkpoint(self):
	      import regex,string
	      import time
	      del self.jou
	      self.logc.send(log_client.INFO, "Start checkpoint for "+self.name)
	      file = open( os.environ['ENSTORE_DB']+"/"+self.name+".jou","r")
	      while 1:
		    l = file.readline()
		    if len(l) == 0 : break
		    if regex.search('del',l):
			t1,t2=string.splitfields(l,' = ')
			exec('key=' + t1[regex.search("\[",t1)+1:regex.search("\]",t1)])
			exec('value=' + t2)
			self.add(key,value)
		    else:
			exec('key=' + l[regex.search("\[",l)+1:regex.search("\]",l)])
			self.delete(key)

	      file.close()
	      cmd="mv " + os.environ['ENSTORE_DB'] +"/"+self.name+".jou " + \
                        os.environ['ENSTORE_DB'] +"/"+self.name+".jou."+ \
                        repr(time.time())
	      os.system(cmd)
	      self.jou = JournalDict({},self.name+".jou")
	      self.count=0
	      self.logc.send(log_client.INFO, "End checkpoint for "+self.name)
	def start_backup(self):
	     global  backup_flag            
	     backup_flag=0
             self.logc.send(log_client.INFO, "Start backup for "+self.name)
             self.checkpoint()
        def stop_backup(self):
	     global  backup_flag           
             backup_flag=1
	     self.logc.send(log_client.INFO, "End backup for "+self.name)
def do_backup(name):
	import time
	cmd="tar cvf "+name+".tar  "+ os.environ['ENSTORE_DB'] + \
                     "/"+name+".dat "+os.environ['ENSTORE_DB'] + \
                     "/"+name+".dir "+ os.environ['ENSTORE_DB'] + \
                     "/"+name+".jou.*"
	print cmd
	os.system(cmd)
	cmd="mv " + name +".tar  /tmp/backup/"+name+".tar."+ \
                        repr(time.time())
	print cmd
	os.system(cmd)
	cmd="rm "+os.environ['ENSTORE_DB']+"/"+name +".jou.*"
	print cmd
	os.system(cmd)
if __name__=="__main__":
	db=dBTable("volume")
	for i in range(1,100):
	  db[i]={'1':'a','2':'b'}
	db.dump()
        key='1'
        if db.has_key(key):
          print "found key",key
        db['200']={'1':'a','2':'b'}
        record=db['2']
        del record['1']
        db['2']=record
        del db['3'] 
        db.checkpoint()
	db.dump()
	db[210]={'1':'a','2':'b'}
	db.dump()
	db.start_backup()
        do_backup("volume")
        db.stop_backup()
	db[211]={'1':'a','2':'b'}
        db[212]={'1':'a','2':'b'}
	db.close()
			




