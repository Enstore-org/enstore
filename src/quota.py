#!/usr/bin/env python

import option
import configuration_client
import pg
import info_client
import generic_client
import sys
import string

MY_NAME = "QUOTA"
MY_SERVER = None

def show_query_result(res):
	result = {}
	result['result'] = res.getresult()
	result['fields'] = res.listfields()
	result['ntuples'] = res.ntuples()
	info_client.show_query_result(result)

class Quota:
	def __init__(self, csc):
		# where is the database
		self.csc = configuration_client.ConfigurationClient(csc)
		dbInfo = self.csc.get('database')
		self.host = dbInfo['db_host']
		self.port = dbInfo['db_port']
		self.dbname = dbInfo['dbname']
		self.db = pg.DB(host=self.host, port=self.port, dbname=self.dbname)

	def show_all(self):
		res = self.db.query("select * from quota order by library;")
		show_query_result(res)

	def show_by_library(self):
		q = "select library, sum(requested) as requested, \
			sum(authorized) as authorized, \
			sum(quota) as quota from quota \
			group by library order by library;"
		show_query_result(self.db.query(q))

	def show(self, library=None, sg=None):
		q = "select value from option where key = 'quota';"
		state = self.db.query(q).getresult()[0][0]
		print "QUOTA is %s\n"%(string.upper(state))
		if library:
			if sg:
				q = "select * from quota where \
					library = '%s' and \
					storage_group = '%s';"%(
					library, sg)
			else:
				q = "select * from quota where \
					library = '%s' \
					order by storage_group;"%(
					library)
		else:
			q = "select * from quota order by library, storage_group;"

		show_query_result(self.db.query(q))

	def check(self, library, sg):
		msg = ""
		q = "select * from quota where library = '%s' and \
			storage_group = '%s';"%(library, sg)
		res = self.db.query(q).dictresult()
		if len(res):
			if res[0]['authorized'] > res[0]['requested']:
				msg = msg+"authorized(%d) > requested(%d)! "%(res[0]['authorized'], res[0]['requested'])
			if res[0]['quota'] > res[0]['authorized']:
				msg = msg+"quota(%d) > authorized(%d)!"%(res[0]['quota'], res[0]['authorized'])
			if msg:
				print "Warning:", msg

	def exist(self, library, sg):
		q = "select * from quota where library = '%s' and \
			storage_group = '%s';"%(library, sg)
		return self.db.query(q).ntuples()

	def create(self, library, sg, requested = 0, authorized = 0,
		quota = 0):
		# check if it already existed
		if self.exist(library, sg):
			print "('%s', '%s') already exists."%(library, sg)
			return

		q = "insert into quota values('%s', '%s', %d, %d, %d);"%(
			library, sg, requested, authorized, quota)
		self.db.query(q)
		self.show(library, sg)
		self.check(library, sg)

		
	def delete(self, library, sg):
		# check if it already existed
		if self.exist(library, sg):
			q = "delete from quota where library = '%s' and \
				storage_group = '%s';"%(library, sg)
			self.db.query(q)
		else:
			print "('%s', '%s') does not exist."%(library, sg)


	def set_requested(self, library, sg, n):
		# check if it already existed
		if self.exist(library, sg):
			q = "update quota set requested = %d where \
				library = '%s' and \
				storage_group = '%s';"%(n, library, sg)
			self.db.query(q)
			self.show(library, sg)
			self.check(library, sg)
		else:
			print "('%s', '%s') does not exist."%(library, sg)

	def set_authorized(self, library, sg, n):
		# check if it already existed
		if self.exist(library, sg):
			q = "update quota set authorized = %d where \
				library = '%s' and \
				storage_group = '%s';"%(n, library, sg)
			self.db.query(q)
			self.show(library, sg)
			self.check(library, sg)
		else:
			print "('%s', '%s') does not exist."%(library, sg)

	def set_quota(self, library, sg, n):
		# check if it already existed
		if self.exist(library, sg):
			q = "update quota set quota = %d where \
				library = '%s' and \
				storage_group = '%s';"%(n, library, sg)
			self.db.query(q)
			self.show(library, sg)
			self.check(library, sg)
		else:
			print "('%s', '%s') does not exist."%(library, sg)

	def enable(self):
		q = "select value from option where key = 'quota';"
		res = self.db.query(q).getresult()
		if res:
			state = res[0][0]
			if state == 'enabled':
				return
			q = "update option set value = 'enabled' where key = 'quota';"
		else:
			q = "insert into option (key, value) values ('quota', 'enabled');"
		self.db.query(q)

	def disable(self):
		q = "select value from option where key = 'quota';"
		res = self.db.query(q).getresult()
		if res:
			state = res[0][0]
			if state == 'disabled':
				return
			q = "update option set value = 'disabled' where key = 'quota';"
		else:
			q = "insert into option (key, value) values ('quota', 'disabled');"
		self.db.query(q)


	def quota_enabled(self):
		q = "select value from option where key = 'quota';"
		state = self.db.query(q).getresult()[0][0]
		if state != "enabled":
			return None
		q = "select library, storage_group, quota from quota;"
		res = self.db.query(q).dictresult()
		libraries = {}
		for i in res:
			if not libraries.has_key(i['library']):
				libraries[i['library']] = {}
			libraries[i['library']][i['storage_group']] = i['quota']
		q_dict = {
			'enabled': 'yes',
			'libraries': libraries
		}

		return q_dict
				

class Interface(option.Interface):
	def __init__(self, args=sys.argv, user_mode=0):
		self.show = None
		self.storage_group = None
		self.show_by_library = None
		self.set_requested = None
		self.set_authorized = None
		self.set_quota = None
		self.create = None
		self.requested = 0
		self.authorized = 0
		self.quota = 0
		self.number = 0
		self.delete = None
		self.enable = None
		self.disable = None

		option.Interface.__init__(self, args=args, user_mode=user_mode)

	def valid_dictionaries(self):
		return (self.help_options, self.quota_options)

	quota_options = {
		option.SHOW:{
			option.HELP_STRING: "show quota",
			option.VALUE_TYPE: option.STRING,
			option.VALUE_USAGE: option.OPTIONAL,
			option.VALUE_LABEL: "library",
			option.DEFAULT_VALUE: "-1",
			option.USER_LEVEL: option.ADMIN,
			option.EXTRA_VALUES: [{
				option.VALUE_NAME: "storage_group",
				option.VALUE_TYPE: option.STRING,
				option.VALUE_USAGE: option.OPTIONAL,
				option.DEFAULT_TYPE: None,
				option.DEFAULT_VALUE: None
			}] },
		option.SHOW_BY_LIBRARY:{
			option.HELP_STRING: "show quota by the libraries",
			option.VALUE_TYPE: option.STRING,
			option.VALUE_USAGE: option.IGNORED,
			option.DEFAULT_TYPE:option.INTEGER,
			option.DEFAULT_VALUE:option.DEFAULT,
			option.USER_LEVEL:option.ADMIN},
		option.SET_REQUESTED:{
			option.HELP_STRING: "set requested number for (library, storage_group)",
			option.VALUE_TYPE: option.STRING,
			option.VALUE_USAGE: option.REQUIRED,
			option.VALUE_LABEL: "library",
			option.USER_LEVEL: option.ADMIN,
			option.EXTRA_VALUES: [{
				option.VALUE_NAME: "storage_group",
				option.VALUE_TYPE: option.STRING,
				option.VALUE_USAGE: option.REQUIRED}, {
				option.VALUE_NAME: "number",
				option.VALUE_TYPE: option.INTEGER,
				option.VALUE_USAGE: option.REQUIRED
			}] },
		option.SET_AUTHORIZED:{
			option.HELP_STRING: "set authorized number for (library, storage_group)",
			option.VALUE_TYPE: option.STRING,
			option.VALUE_USAGE: option.REQUIRED,
			option.VALUE_LABEL: "library",
			option.USER_LEVEL: option.ADMIN,
			option.EXTRA_VALUES: [{
				option.VALUE_NAME: "storage_group",
				option.VALUE_TYPE: option.STRING,
				option.VALUE_USAGE: option.REQUIRED}, {
				option.VALUE_NAME: "number",
				option.VALUE_TYPE: option.INTEGER,
				option.VALUE_USAGE: option.REQUIRED
			}] },
		option.SET_QUOTA:{
			option.HELP_STRING: "set quota for (library, storage_group)",
			option.VALUE_TYPE: option.STRING,
			option.VALUE_USAGE: option.REQUIRED,
			option.VALUE_LABEL: "library",
			option.USER_LEVEL: option.ADMIN,
			option.EXTRA_VALUES: [{
				option.VALUE_NAME: "storage_group",
				option.VALUE_TYPE: option.STRING,
				option.VALUE_USAGE: option.REQUIRED}, {
				option.VALUE_NAME: "number",
				option.VALUE_TYPE: option.INTEGER,
				option.VALUE_USAGE: option.REQUIRED
			}] },
		option.CREATE:{
			option.HELP_STRING: "create quota for (library, storage_group)",
			option.VALUE_TYPE: option.STRING,
			option.VALUE_USAGE: option.REQUIRED,
			option.VALUE_LABEL: "library",
			option.USER_LEVEL: option.ADMIN,
			option.DEFAULT_VALUE: "hello",
			option.EXTRA_VALUES: [{
				option.VALUE_NAME: "storage_group",
				option.VALUE_TYPE: option.STRING,
				option.VALUE_USAGE: option.REQUIRED}, {
				option.VALUE_NAME: "requested",
				option.VALUE_TYPE: option.INTEGER,
				option.DEFAULT_VALUE: 0,
				option.VALUE_USAGE: option.OPTIONAL}, {
				option.VALUE_NAME: "authorized",
				option.VALUE_TYPE: option.INTEGER,
				option.DEFAULT_VALUE: 0,
				option.VALUE_USAGE: option.OPTIONAL}, {
				option.VALUE_NAME: "quota",
				option.VALUE_TYPE: option.INTEGER,
				option.DEFAULT_VALUE: 0,
				option.VALUE_USAGE: option.OPTIONAL
			}] },
		option.DELETE:{
			option.HELP_STRING: "delete (library, storage_group)",
			option.VALUE_TYPE: option.STRING,
			option.VALUE_USAGE: option.REQUIRED,
			option.VALUE_LABEL: "library",
			option.USER_LEVEL: option.ADMIN,
			option.EXTRA_VALUES: [{
				option.VALUE_NAME: "storage_group",
				option.VALUE_TYPE: option.STRING,
				option.VALUE_USAGE: option.REQUIRED
			}] },
		option.ENABLE:{
			option.HELP_STRING: "enable quota",
			option.DEFAULT_VALUE:option.DEFAULT,
			option.DEFAULT_TYPE:option.INTEGER,
			option.VALUE_USAGE:option.IGNORED,
			option.USER_LEVEL: option.ADMIN},
		option.DISABLE:{
			option.HELP_STRING: "disable quota",
			option.DEFAULT_VALUE:option.DEFAULT,
			option.DEFAULT_TYPE:option.INTEGER,
			option.VALUE_USAGE:option.IGNORED,
			option.USER_LEVEL: option.ADMIN},
			
	}
			
if __name__ == '__main__':
	intf = Interface(user_mode=0)
	q = Quota((intf.config_host, intf.config_port))

	if intf.show:
		if intf.show == "-1":
			q.show()
		else:
			if intf.storage_group:
				q.show(intf.show, intf.storage_group)
			else:
				q.show(intf.show)
	elif intf.show_by_library:
		q.show_by_library()
	elif intf.create:
		if intf.requested == 'None':
			intf.requested = 0
		if intf.authorized == 'None':
			intf.authorized = 0
		if intf.quota == 'None':
			intf.quota = 0
		q.create(intf.create, intf.storage_group,
			intf.requested, intf.authorized, intf.quota)
	elif intf.set_requested:
		q.set_requested(intf.set_requested, intf.storage_group,
			intf.number)
	elif intf.set_authorized:
		q.set_authorized(intf.set_authorized, intf.storage_group,
			intf.number)
	elif intf.set_quota:
		q.set_quota(intf.set_quota, intf.storage_group,
			intf.number)
	elif intf.delete:
		q.delete(intf.delete, intf.storage_group)
	elif intf.disable:
		q.disable()
	elif intf.enable:
		q.enable()
