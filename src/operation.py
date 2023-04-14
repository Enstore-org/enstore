#!/usr/bin/env python
"""
Schema

 Schema |       Name        |   Type   |  Owner
--------+-------------------+----------+---------
 public | job               | table    | huangch
 public | job_definition    | table    | huangch
 public | job_definition_id | sequence | huangch
 public | job_id            | sequence | huangch
 public | object            | table    | huangch
 public | progress          | table    | huangch
 public | task              | table    | huangch
 public | task_id           | sequence | huangch

                                Table "public.job"
 Column  |            Type             |                Modifiers
---------+-----------------------------+------------------------------------------
 id      | integer                     | not null default nextval('job_id'::text)
 name    | character varying           | not null
 type    | integer                     | not null
 start   | timestamp without time zone | default now()
 finish  | timestamp without time zone |
 comment | character varying           |
 id2     | bigint                      | not null
Indexes:
    "job_pkey" PRIMARY KEY, btree (id)
    "job_id2_key" UNIQUE, btree (id2)
    "job_finish_idx" btree (finish)
    "job_name_idx" btree (name)
    "job_start_idx" btree ("start")
    "job_type_idx" btree ("type")
Foreign-key constraints:
    "job_type_fkey" FOREIGN KEY ("type") REFERENCES job_definition(id) ON UPDATE CASCADE ON DELETE CASCADE

                           Table "public.job_definition"
 Column  |       Type        |                      Modifiers
---------+-------------------+-----------------------------------------------------
 id      | integer           | not null default nextval('job_definition_id'::text)
 name    | character varying | not null
 tasks   | integer           |
 remarks | character varying |
Indexes:
    "job_definition_pkey" PRIMARY KEY, btree (id)
    "job_definition_name_idx" btree (name)

                            Table "public.task"
   Column   |       Type        |                 Modifiers
------------+-------------------+-------------------------------------------
 id         | integer           | not null default nextval('task_id'::text)
 seq        | integer           | not null
 job_type   | integer           | not null
 dsc        | character varying |
 action     | character varying |
 comment    | character varying |
 auto_start | character(1)      | default 'm'::bpchar
Indexes:
    "task_pkey" PRIMARY KEY, btree (seq, job_type)
Foreign-key constraints:
    "task_job_type_fkey" FOREIGN KEY (job_type) REFERENCES job_definition(id) ON UPDATE CASCADE ON DELETE CASCADE

                Table "public.progress"
 Column  |            Type             |   Modifiers
---------+-----------------------------+---------------
 job     | integer                     | not null
 task    | integer                     | not null
 start   | timestamp without time zone | default now()
 finish  | timestamp without time zone |
 comment | character varying           |
 args    | character varying           |
 result  | character varying           |
Indexes:
    "progress_job_idx" btree (job)
    "progress_start_idx" btree ("start")
Foreign-key constraints:
    "progress_job_fkey" FOREIGN KEY (job) REFERENCES job(id) ON UPDATE CASCADE ON DELETE CASCADE

         Table "public.object"
 Column |       Type        | Modifiers
--------+-------------------+-----------
 job    | integer           | not null
 object | character varying |
Indexes:
    "object_job_idx" btree (job)
    "object_object_idx" btree ("object")
Foreign-key constraints:
    "object_job_fkey" FOREIGN KEY (job) REFERENCES job(id) ON UPDATE CASCADE ON DELETE CASCADE

"""

import os
import pprint
import pwd
import time
import types
import smtplib
import stat
import sys
# enstore import

import configuration_client
import e_errors
import enstore_functions2
import dbaccess

try:
	import snow_fliptab
except:
	pass
debug = False
# debug = True
csc = {}

# timestamp2time(ts) -- convert "YYYY-MM-DD HH:MM:SS" to time
def timestamp2time(s):
	if s == '1969-12-31 17:59:59':
		return -1
	else:
		# take care of daylight saving time
		tt = list(time.strptime(s, "%Y-%m-%d %H:%M:%S"))
		tt[-1] = -1
		return time.mktime(tuple(tt))

# time2timestamp(t) -- convert time to "YYYY-MM-DD HH:MM:SS"
def time2timestamp(t):
	return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t))

# is_time(t) -- check if t is of time type "YYYY-MM-DD_hh:mm:ss"
# 		got to handle space
def is_time(t):
	if len(t) != 19:
		return False
	if t[4] == '-' and t[7] == '-' and (t[10] == '_' or t[10] == ' ') and \
		t[13] == ':'and t[16] == ':':
		return True
	else:
		return False

# send_mail(subject, message) -- simplified sendmail
def send_mail(subject, message):
	from_addr = pwd.getpwuid(os.getuid())[0]+'@'+os.uname()[1]
	if os.environ['ENSTORE_MAIL']:
		to_addr = os.environ['ENSTORE_MAIL']
	else:
		to_addr = "enstore-admin@fnal.gov"
	msg = [	"From: %s"%(from_addr),
		"To: %s"%(to_addr),
		"Subject: %s"%(subject),
		""] + message
	return smtplib.SMTP('localhost').sendmail(from_addr, [to_addr], '\n'.join(msg))

TEMP_DIR = '/tmp/operation'
# make it if it is not there
if not os.access(TEMP_DIR, os.F_OK):
	os.makedirs(TEMP_DIR)

def clean_up_temp_dir():
	for i in os.listdir(TEMP_DIR):
		os.remove(os.path.join(TEMP_DIR, i))

DATABASEHOST = 'stkensrv0n.fnal.gov'
#DATABASEHOST = 'localhost'
DATABASEPORT = 8800
DATABASENAME = 'operation'
DATABASEUSER = None

# This is a hard wired configuration
def library_type(cluster, lib):
	if cluster == 'D0':
		if lib in ('D0-9940B','mezsilo'):
			return '9310'
		if lib in ('samlto2','samlto'):
			return 'aml2'
		if lib in ('D0-LTO4F1','D0-10KCF1'):
			return '8500F1'
		if lib in ('D0-LTO4G1',):
			return '8500G1'
		if lib in ('D0-LTO4GS','D0-10KCGS'):
			return '8500GS'
	elif cluster == 'STK':
		if lib in ('CD-9940B','9940'):
			return '9310'
		if lib in ('CD-LTO3','CD-LTO4G1','CD-10KCG1','CD-10KDG1'):
			return '8500G1'
		if lib in ('CD-LTO3GS','CD-LTO4GS','CD-10KCGS','CD-10KDGS'):
			return '8500GS'
		if lib in ('CD-LTO4F1','CD-10KCF1','CD-10KDF1'):
			return '8500F1'
	elif cluster == 'CDF':
		if lib in ('CDF-9940B','cdf'):
			return '9310'
		if lib in ('CDF-LTO3','CDF-LTO4G1'):
			return '8500G1'
		if lib in ('CDF-LTO4GS','CDF-10KCGS'):
			return '8500GS'
		if lib in ('CDF-LTO4F1','CDF-10KCF1'):
			return '8500F1'
	elif cluster == 'GCC':
		if lib in ('LTO3','LTO4','10KCG1'):
			return '8500G1'
		if lib == 'LTO4F1':
			return '8500F1'
	else:
		return None

# get_cluster(host) -- determine current cluster
def get_cluster(host):
	if host[:2] == 'd0':
		return 'D0'
	elif host[:3] == 'stk' or host[:7] == 'enstore':
		return 'STK'
	elif host[:3] == 'cdf':
		return 'CDF'
	elif host[:3] == 'gcc':
		return 'GCC'
	else:
		return None

# get_script_host(cluster) -- determine script host
def get_script_host(cluster):
	if cluster.upper()[:2] == 'D0':
		return 'd0ensrv4n.fnal.gov'
	elif cluster.upper()[:3] == 'STK':
		return 'stkensrv4n.fnal.gov'
	elif cluster.upper()[:3] == 'CDF':
		return 'cdfensrv4n.fnal.gov'
	elif cluster.upper()[:3] == 'GCC':
		return 'gccensrv2.fnal.gov'
	else:
		return 'localhost'

# get_write_protect_script_path(library_type) -- determine script path
def get_write_protect_script_path(lib_type):
	if lib_type in ['9310', 'aml2', '8500G1', '8500GS', '8500F1']:
		return  '/home/enstore/isa-tools/' + lib_type + '_write_protect_work'
	else:
		return '/tmp'

# get_write_permit_script_path(library_type) -- determine script path
def get_write_permit_script_path(lib_type):
	if lib_type in ['9310', 'aml2', '8500G1', '8500GS', '8500F1']:
		return  '/home/enstore/isa-tools/' + lib_type + '_write_permit_work'
	else:
		return '/tmp'

# get_default_library(cluster)
def get_default_library(cluster):
	if cluster == 'STK':
		return "9940,CD-9940B"
	elif cluster == 'CDF':
		return "cdf,CDF-9940B"
	elif cluster == 'D0':
		return "mezsilo,D0-9940B"
	elif cluster == 'GCC':
		return "LTO3"
	else:
		return "unknown"

# get_qualifier(library_type) -- determine name qualifier
def get_qualifier(lib_type):
	if lib_type == 'aml2':
		return 'a'
	elif lib_type == '8500GS':
		return 'r'
	elif lib_type == '8500G1':
		return 's'
	elif lib_type == '8500F1':
		return 't'
	else:
		return ''

csc = configuration_client.ConfigurationClient((enstore_functions2.default_host(),
						enstore_functions2.default_port()))
enstoredb = csc.get('database')
operation_db = csc.get('operation_db')

if operation_db['status'][0] == e_errors.OK:
	DATABASENAME = operation_db['dbname']
	DATABASEPORT = operation_db['dbport']
	DATABASEHOST = operation_db['dbhost']
	DATABASEUSER = operation_db['dbuser']
elif enstoredb['dbhost'].find('.fnal.gov') == -1:
	print "no database host defined for this node"
	sys.exit(0)

cluster = get_cluster(enstoredb['db_host'])
script_host = get_script_host(cluster)
DEFAULT_LIBRARIES = get_default_library(cluster)

# get_db() -- initialize a database connection
def get_db():
	return dbaccess.DatabaseAccess(maxconnections=1,
				       database=DATABASENAME,
				       host=DATABASEHOST,
				       port=DATABASEPORT,
				       user=DATABASEUSER)

def get_edb(enstoredb):
	return dbaccess.DatabaseAccess(maxconnections=1,
				       database=enstoredb['dbname'],
				       host=enstoredb['db_host'],
				       port=enstoredb['db_port'],
				       user=enstoredb['dbuser'])

# global db connection
db = get_db()
edb = get_edb(enstoredb)

npl = 10 # number of items per line

def show_cap(header, list):
	label = "CAP %d:"%(header)
	print label,
	nl = 1
	for i in list:
		print i,
		if nl % npl == 0:
			print
			print " "*len(label),
		nl = nl + 1
	print

# get_rem_ticket_number(rem_res)
#	get ticket number from remedy API
#	rem_res is the result (array of lines) from remedy API
def get_rem_ticket_number(rem_res):
	for i in rem_res:
		t = i.split()
		if len(t) > 5:
			if t[0] == 'Entry' and \
				t[1] == 'created' and \
				t[2] == 'with' and \
				t[3] == 'id' and \
				t[4] == '=':
				return "HELPDESK_TICKET_"+t[5]
	return 'UNKNOWN_TICKET'

# get_unfinished_job(cluster) -- get unfinished job of certain cluster
def get_unfinished_job(cluster=None):
	res = None
	if cluster:
		q = "select name from job where name ilike '%s%%' and finish is null"%(cluster)
		res = db.query_getresult()
	else:
		q = "select name from job where finish is null"
	res = db.query_getresult(q)
	jobs = []
	for i in res:
		jobs.append(i[0])
	return jobs

# decode_job(job) -- decode the type of job from its name
def decode_job(job):
	if job[:3] == 'STK' or job[:3] == "CDF":
		cluster = job[:3]
		if job[3] in ['a', 'r', 's', 't']:
			if job[3] == 'a':
				lt = 'aml2'
			elif job[3] == 'r':
				lt = '8500GS'
			elif job[3] == 's':
				lt = '8500G1'
			elif job[3] == 't':
				lt = '8500F1'
			else:
				lt = 'unknown'
			type = job[5]
			t = job[6:].split('-')
			job_range = range(int(t[0]), int(t[1])+1)
		else:
			lt = '9310'
			type = job[4]
			t = job[5:].split('-')
			job_range = range(int(t[0]), int(t[1])+1)
	elif job[:2] == 'D0':
		cluster = job[:2]
		if job[2] in ['a', 'r', 's', 't']:
			if job[2] == 'a':
				lt = 'aml2'
			elif job[2] == 'r':
				lt = '8500GS'
			elif job[2] == 's':
				lt = '8500G1'
			elif job[2] == 't':
				lt = '8500F1'
			else:
				lt = 'unknown'
			type = job[4]
			t = job[5:].split('-')
			job_range = range(int(t[0]), int(t[1])+1)
		else:
			lt = '9310'
			type = job[3]
			t = job[4:].split('-')
			job_range = range(int(t[0]), int(t[1])+1)
	return cluster, type, job_range, lt

# is_done(job) -- is this job done?
def is_done(job):
	c, t, r, lt = decode_job(job)
	if c != cluster:	# not on this cluster
		return 0
	if t == 'E':	# write enable
		p = get_write_permit_script_path(lt)
	elif t == 'P':	# write protect
		p = get_write_protect_script_path(lt)
	else:		# don't know
		if debug:
			print "unknown job", job, c, t, `r`
		return 0
	t0 = 0

	for i in r:
		pp = os.path.join(p, `i`)
		if os.access(pp, os.F_OK):
			return 0
		elif os.access(pp+'.done', os.F_OK):
			t1 = os.stat(pp+'.done')[stat.ST_CTIME]
			if t1 > t0:
				t0 = t1
		else:
			return 0
	return t0

# try_close_all(cluster) -- try close open job in cluster
def try_close_all(cluster):
	j_list = get_unfinished_job(cluster)
	msg = []
	for i in j_list:
		t = is_done(i)
		if t:
			print i, "is done at", time.ctime(t)
			finish_current_task(i, result='DONE', comment='AUTO-CLOSE', timestamp=time2timestamp(t))
			print i, "is closed at", time.ctime(time.time())
			msg.append("%s is closed with timestamp %s"%(
				i, time.ctime(t)))
		else:
			print i, "is not done yet"
	if msg:
		send_mail("Closing tab-flipping job(s)", msg)

# auto_close_all() -- automatically close all finished jobs
def auto_close_all():
	global cluster
	if os.uname()[1] != script_host:
		print "Wrong host %s (%s)"%(os.uname()[1], script_host)
		return
	try_close_all(cluster)

# create_job() -- generic job creation
def create_job(name, type, args, comment = ''):
	association = None
	# check if any of the args are in open job
	problem_args = {}
	for i in args:
		q = "select job.id, job.name, job_definition.name as job_def, \
			job.start from job, job_definition, object \
			where \
				object.object = '%s' and \
				job.id = object.job and \
				job.finish is null and \
				job.type = job_definition.id"%(i)
		if debug:
			print q
		res = db.query_getresult(q)
		if res:
			problem_args[i] = res[0]

	if problem_args:
		for i in problem_args.keys():
			print "%s is already in unfinished job %d %s %s %s"%(
				i,
				problem_args[i]['id'],
				problem_args[i]['name'],
				problem_args[i]['job_def'],
				problem_args[i]['start'])
		return -1

	# is there a time stamp specified?
	if is_time(args[0]):
		q = "insert into job (name, type, start, comment) \
			values ('%s', \
				(select id from job_definition where \
				name = '%s'), '%s', '%s');"%(
			name, type, args[0].replace('_', ' '), comment)
		args = args[1:]
	else:
		q = "insert into job (name, type, comment) \
			values ('%s', (select id from job_definition where \
				name = '%s'), '%s');"%(
			name, type, comment)
	if debug:
		print q
	db.insert(q)
	# get job id
	q = "select id from job where name = '%s';"%(
		name)
	if debug:
		print q
	id = db.query_getresult(q)[0][0]
	for i in args:
		# is it association setting?
		if i[-1] == ':':
			association = i[:-1]
		else:
			# does it have embedded association?
			p = i.split(':')
			if len(p) > 1:	# yes
				q = "insert into object (job,object,association) values (%d, '%s', '%s');"%(id, p[1], p[0])
			else:		# nope
				if association:
					q = "insert into object (job, object, association) values (%d, '%s', '%s');"%(id, i, association)
				else:
					q = "insert into object (job, object) values (%d, '%s');"%(id, i)
		if debug:
			print q
		db.insert(q)
	return id

# get_job_by_name() -- from a name to find the job; name is unique
def get_job_by_name(name):
	q = "select * from job where name = '%s';"%(name)
	if debug:
		print q
	res = db.query_dictresult(q)
	if res:
		return retrieve_job(res[0])
	else:
		return None

# get_job_by_id() -- get_job_using internal id
def get_job_by_id(id):
	q = "select * from job where id = %d;"%(id)
	if debug:
		print q
	res = db.query_dictresult(q)
	if res:
		return retrieve_job(res[0])
	else:
		return None

# get_job_by_time() -- get job using time frame
def get_job_by_time(after, before = None):
	if not before:	# default now()
		before = time2timestamp(time.time())
	if type(after) != types.StringType:
		after = time2timestamp(after)
	if type(before) != types.StringType:
		before = time2timestamp(before)
	q = "select * from job where start >= '%s' and start <= '%s' \
		order by start;"%(after, before)
	if debug:
		print q
	res = db.query_dictresult(q)
	if res:
		return retrieve_job(res[0])
	else:
		return None

# retrieve_job() -- get all related information of this job
def retrieve_job(job):
	# assemble its related objects
	q = "select * from object where job = %d order by association, object;"%(job['id'])
	object = {}
	if debug:
		print q
	res = db.query_getresult(q)
	for j in res:
		if object.has_key(j[2]):
			object[j[2]].append(j[1])
		else:
			object[j[2]] = [j[1]]
	job['object'] = object
	# list its related tasks
	q = "select * from job_definition where id = %d;"%(job['type'])
	if debug:
		print q
	job_definition = db.query_dictresult(q)[0]
	job['job_definition'] = job_definition
	q = "select * from task left outer join progress \
		on (progress.job = %d and task.seq = progress.task) \
		where task.job_type = %d \
			order by seq;"%(job['id'], job['type'])
	if debug:
		print q
	job['task'] = db.query_dictresult(q)
	job['current'] = get_current_task(job['name'])
	job['next'] = get_next_task(job['name'])
	if job['finish']:
		job['status'] = 'finished'
	else:
		if job['current'] == 0:
			job['status'] = 'not_started'
		else:
			job['status'] = 'in_progress'
	return job

# get_job_tasks(name) -- show the tasks related to this job
def get_job_tasks(name):
	q = "select seq, dsc, action from task, job \
		where job.name = '%s' and job.type = task.job_type \
		order by seq;"%(name)
	if debug:
		print q
	return db.query_dictresult(q)

# start_job_task(job_name, task_id) -- start a task
def start_job_task(job_name, task_id, args=None, comment=None, timestamp=None):
	if has_started(job_name, task_id):
		return "job %s task %d has already started"%(job_name, task_id)

	if args:
		args = "'%s'"%(args)
	else:
		args = "null"
	if comment:
		comment = "'%s'"%(comment)
	else:
		comment = "null"
	if timestamp:
		q = "insert into progress (job, task, start, comment, args) \
		values ((select id from job where name = '%s'), %d, '%s', %s, %s);"%(
			job_name, task_id, timestamp, comment, args)
	else:
		q = "insert into progress (job, task, comment, args) \
		values ((select id from job where name = '%s'), %d, %s, %s);"%(
			job_name, task_id, comment, args)
	if debug:
		print q
	res = db.insert(q)
	return `res`

# finish_job_task(job_name, task_id) -- finish/close a task
def finish_job_task(job_name, task_id, comment=None, result=None, timestamp=None):
	if not has_started(job_name, task_id):
		return "job %s task %d has not started"%(job_name, task_id)
	if has_finished(job_name, task_id):
		return "job %s task %d has lready finished"%(job_name, task_id)
	if result:
		result = "'%s'"%(str(result))
	else:
		result = "null"

	if not timestamp:
		timestamp = "now()"
	else:
		timestamp = "'%s'"%(timestamp)
	if comment:
		q = "update progress \
			set finish = %s, comment = '%s', \
				result = %s \
			where job = (select id from job where name = '%s') \
			and task = %d;"%(
			timestamp, comment, result, job_name, task_id)
	else:
		q = "update progress \
			set finish = %s, result = %s \
			where job = (select id from job where name = '%s') \
			and task = %d"%(
			timestamp, result, job_name, task_id)
	if debug:
		print q
	res = db.insert(q)
	return `res`

# get_current_task(name) -- get current task
def get_current_task(name):
	q = "select case \
		when max(task) is null then 0 \
		else max(task) \
		end \
		from progress, job \
		where \
			job.name = '%s' and \
			progress.job = job.id and \
			progress.start is not null;"%(name)
	if debug:
		print q
	res = db.query_getresult(q)
	return res[0][0]

# get_next_task(name) -- get next task
def get_next_task(name):
	q = "select tasks, finish from job, job_definition where \
		job.name = '%s' and \
		job.type = job_definition.id;"%(name)
	if debug:
		print q
	res = db.query_getresult(q)
	tasks = res[0][0]
	finish = res[0][1]
	if finish:
		return 0
	ct = get_current_task(name)
	if ct == tasks:
		return 0
	else:
		return ct + 1

# has_finished(job, task) -- has task (job, task) finished?
def has_finished(job, task):
	q = "select p.finish from progress p, job j where \
		j.name = '%s' and \
		p.job = j.id and p.task = %d and p.finish is not null;"%(
		job, task)
	if debug:
		print q
	res = db.query_getresult(q)
	if not res:
		return False
	else:
		return True

# is_started(job, task) -- has task (job, task) started?
def has_started(job, task):
	q = "select p.start from progress p, job j where \
		j.name = '%s' and \
		p.job = j.id and p.task = %d and p.start is not null;"%(
		job, task)
	if debug:
		print q
	res = db.query_getresult(q)
	if not res:
		return False
	else:
		return True

# start_next_task(job) -- start next task
def start_next_task(job, args=None, comment=None, timestamp=None):
	res = []
	ct = get_current_task(job)
	nt = get_next_task(job)
	if nt:
		if ct == 0 or has_finished(job, ct):
			res2 = start_job_task(job, nt, args, comment, timestamp)
			if res2:
				res.append(res2)
		else:
			res.append('current task has not finished')
	else:
		res.append('no more tasks')
	return res

# finish_current_task(job) -- finish current task
def finish_current_task(job, result = None, comment = None, timestamp=None):
	res = []
	ct = get_current_task(job)
	if ct:
		if has_finished(job, ct):
			res.append('current task has already finished')
		else:
			res2 = finish_job_task(job, ct, comment, result, timestamp)
			if res2:
				res.append(res2)
	else:
		res.append('no current task')
	return res

# show_current_task(job) -- show current task of job
def show_current_task(j):
	job = get_job_by_name(j)
	if not job:
		return "%s does not exist"%(j)
	if job['status'] == "not_started":
		return "%s has not started"%(j)
	q = "select d.name as desc, t.seq, \
		t.dsc, t.action, \
		p.start, p.finish \
		from job j, job_definition d, \
			task t, progress p \
		where \
			j.id = %d and \
			t.seq = %d and \
			t.job_type = d.id and \
			p.job = j.id and \
			p.task = t.seq and \
			j.type = d.id;"%(
			job['id'], job['current'])
	if debug:
		print q
	ct = db.query_dictresult(q)[0]
	if ct['finish'] == None:
		ct['finish'] = ""
	if ct['action'] == None:
		ct['action'] = 'default'
	return "%s\t%s\t%3d %s\t(%s)\t%s - %s"%(
		j, ct['desc'], ct['seq'],
		ct['dsc'], ct['action'],
		ct['start'], ct['finish'])

# show_next_task(job) -- show next task of job
def show_next_task(j):
	job = get_job_by_name(j)
	if not job:
		return "%s does not exist"%(j)
	if job['status'] == "finished":
		return "%s has finished"%(j)
	if job['next'] == 0:
		return "%s is on the last step"%(j)
	q = "select d.name as desc, t.seq, \
		t.dsc, t.action \
		from job j, job_definition d, \
			task t \
		where \
			j.id = %d and \
			t.seq = %d and \
			t.job_type = d.id and \
			j.type = d.id;"%(
			job['id'], job['next'])
	if debug:
		print q
	ct = db.query_dictresult(q)[0]
	if ct['action'] == None:
		ct['action'] = 'default'
	return "%s\t%s\t%3d %s\t(%s)"%(
		j, ct['desc'], ct['seq'],
		ct['dsc'], ct['action'])
# show(job) -- display a job
def show(job):
	if not job:
		return
	print
	print "   Name: %s"%(job['name'])
	print "   Type: %s (%s)"%(job['job_definition']['name'],
		job['job_definition']['remarks'])
	print " Status: %s"%(job['status'])
	print "  Start: %s"%(job['start'])
	print " Finish: %s"%(job['finish'])
	print " #tasks: %d"%(job['job_definition']['tasks'])
	print "  Tasks:"
	print "Current: %d"%(job['current'])
	print "   Next: %d"%(job['next'])
	for t in job['task']:
		if t['action'] == None:
			t['action'] = "default"
		print "%3d %s %40s (%s) %s %s %s %s %s"%(
			t['seq'], t['auto_start'], t['dsc'],
			t['action'], t['start'], t['finish'], t['args'],
			t['result'], t['comment'])
	print "Objects:"
	for i in job['object'].keys():
		print i+':',
		for j in job['object'][i]:
			print j,
		print

# delete(job) -- delete a job
def delete(job):
	if job:
		q = "delete from job where name = '%s';"%(job)
		if debug:
			print q
		db.delete(q)

def create_write_protect_on_job(name, args, comment = ''):
	return create_job(name, 'WRITE_PROTECTION_TAB_ON', args, comment)

def create_write_protect_off_job(name, args, comment = ''):
	return create_job(name, 'WRITE_PROTECTION_TAB_OFF', args, comment)

# help(topic) -- help function
def help(topic=None):
	if not topic:
		print "operation.py create write_protect_on|write_protect_off <job> [[<association>:] [<associate>:]<object>]+"
		print "    -- creating a job"
		print "operation.py list [all|open|finished|closed|completed|<job>+|has <object>]|recent <n>"
		print "    -- list job(s)"
		print "operation.py show <job>+"
		print "    -- show details of job(s)"
		print "operation.py current <job>+"
		print "    -- show current task(s) of <job>(s)"
		print "operation.py next <job>+"
		print "    -- show next task(s) of <job>(s)"
		print "operation.py start <job> [<arg>]"
		print "    -- start the next task of <job>"
		print "operation.py finish <job> [<result>]"
		print "    -- finish the current task of <job>"
		print "operation.py delete <job>+ [sincerely]"
		print "    -- delete <job>(s)"
		print "operation.py find|locate <object>+"
		print "    -- find jobs that have <object>"
		print "operation.py find+|locate+ <objects>+"
		print "    -- find|locate with details"
		print "operation.py relate <job>+"
		print "    -- find jobs that have common objects"
		print "operation.py recommend_write_protect_on [<library_list>] [limit <n>]"
		print "    -- recommend volumes for flipping write protect tab on"
		print "operation.py recommend_write_protect_off [<library_list>] [limit <n>]"
		print "    -- recommend volumes for flipping write protect tab off"
		print "operation.py auto_write_protect_on [<library_list>] [no_limit]"
		print "    -- automatically generate helpdesk ticket for flipping WP on"
		print "operation.py auto_write_protect_off [<library_list>] [no_limit]"
		print "    -- automatically generate helpdesk ticket for flipping WP off"
		print "operation.py auto_close_all"
		print "    -- try to close all finished open jobs on this cluster"
		print
		print "try:"
		print "operation.py help <topic>"
	elif topic == "create":
		print
		print "operation.py create write_protect_on|write_protect_off <job> [[<association>:] [<associate>:]<object>]+"
		print
		print "<job> is a user defined unique name of the job"
		print "<association> is a way to group objects. By default, there is no association"
		print "<association>: change the association for the rest of the objects in list"
		print "     It is allowed to have multiple <association>: in the command."
		print "     Each <association>: changes the global association setting."
		print "<association>:<object> temporarily override default association for <object>"
		print
		print "EXAMPLE:"
		print "operation.py create write_protect_on WP3 CAP3:  VO2093 VO2094 VO2095 VO2096 VO2097 VO2098 VO2099 VO2152 VO2154 VO2195 VO2196 VO2197 VO2198 VO2199 VO2203 VO2206 VO2207 VO2208 VO2209 VO2211 VO2213 CAP4: VO2224 VO2225 VO2226 VO2227 VO2245 VO2246 VO2252 VO2253 VO2254 VO2256 VO2257 VO2258 VO2259 VO2501 VO2532 VO2533 VO2534 VO2540 VO2541 VO2542 VO2544"
	elif topic == "list":
		print
		print "operation.py list [all|open|finished|closed|completed|<job>+|has <object>]"
		print
		print "all: list all jobs"
		print "open: list all open (not closed) jobs"
		print "finished|closed|completed: list all completed jobs. finished|closed|completed are the same thing"
		print "<job>+ : list named jobs"
		print "has <object>: list all jobs that have <object> as an argument"
	elif topic == "show":
		print
		print "operation.py show <job>+"
		print
		print "show details of <job>s in the list. <job> is addressed by its unique name"
	elif topic == "current":
		print
		print "operation.py current <job>+"
		print
		print "show the current task of <job>."
		print "A current task is one that has stared but its next task has not started"
		print "A job can have at most one such task at any time"
		print "in case of a not yet started job, current task is task 0"
		print "in case of a finished job, current task is the last task"
	elif topic == "next":
		print
		print "operation.py next <job>+"
		print
		print "show next task of <job>"
		print "next task is one that has not started and its previous task has finished."
		print "in case of a have-not-started job, next task is the first task."
		print "in case of a finished job, next task is task 0"
	elif topic == "start":
		print
		print "operation.py start <job> [<arg>]"
		print
		print "start the next task of <job> with optional argument"
		print "next task can start only if current task has finished"
		print
		print "EXAMPLE:"
		print "operation.py start STKWP3 <help_desk_ticket_id>"
	elif topic == "finish":
		print
		print "operation.py finish <job> [<result>]"
		print
		print "finish current task of <job> with optional <result>"
		print "EXAMPLE:"
		print "operation.py finish STKWP3 DONE"
	elif topic == "delete":
		print
		print "operation.py delete <job>+ [sincerely]"
		print
		print "delete <job> in the list"
		print "this is a dangerous command, use with extra care"
		print '<job>s will not be deleted unless "sincerely" is specified at the end'
	elif topic == "find" or topic == "locate":
		print
		print "operation.py find|locate <object>+"
		print
		print "list the jobs that have <object> as an argument"
	elif topic == "find+" or topic == "locate+":
		print
		print "operation.py find+|locate+ <object>+"
		print
		print "same as find|locate but show details of the jobs"
	elif topic == "recommend_write_protect_on" or topic == "recommend_write_protect_off":
		print
		print "operation.py recommend_write_protect_on [<library_list>] [limit <n>]"
		print "operation.py recommend_write_protect_off [<library_list>] [limit <n>]"
		print
		print "list recommended volumes for write protect tab flipping on/off"
		print
		print "<library_list> is a list of media types separated by comma ','"
		print "when <library_list> is omitted, the default list takes place"
		print
		print "with 'limit <n>', it only lists, at most, first <n> volumes for the job"
		print "otherwise, it lists all"
		print
		print "EXAMPLES:"
		print "operation.py recommend_write_protect_on"
		print "operation.py recommend_write_protect_on 9940,CD-9940B"
		print "operation.py recommend_write_protect_on limit 100"
		print "operation.py recommend_write_protect_on 9940,CD-9940B limit 100"
		print "operation.py recommend_write_protect_off"
		print "operation.py recommend_write_protect_off 9940,CD-9940B"
		print "operation.py recommend_write_protect_off limit 100"
		print "operation.py recommend_write_protect_off 9940,CD-9940B limit 100"
	elif topic == "auto_write_protect_on" or topic == "auto_write_protect_off":
		print
		print "operation.py auto_write_protect_on [<library_list>] [no_limit]"
		print "operation.py auto_write_protect_off [<library_list>] [no_limit]"
		print
		print "from recommended list, create a job for write protect tab flipping on/off"
		print "and generate a helpdesk ticket automatically"
		print
		print "<library_list> is a list of media types separated by comma ','"
		print
		print "there is a default limit of 10 caps (220 volume)"
		print "with 'no_limit', it generates everything in one ticket"
		print
		print "EXAMPLES:"
		print "operation.py auto_write_protect_on"
		print "operation.py auto_write_protect_on 9940,CD-9940B"
		print "operation.py auto_write_protect_on no_limit"
		print "operation.py auto_write_protect_on 9940,CD-9940B no_limit"
		print "operation.py auto_write_protect_off"
		print "operation.py auto_write_protect_off 9940,CD-9940B"
		print "operation.py auto_write_protect_off no_limit"
		print "operation.py auto_write_protect_off 9940,CD-9940B no_limit"
	elif topic == 'auto_close_all':
		print
		print "operation.py auto_close_all"
		print
		print "try to close all finished open jobs on this cluster"
		print
		print "this command is meant for script/cronjob or experts!!"
	else:
		print "don't know anything about %s"%(topic)
		print
		help()

# even(i) -- True is i is an even number
def even(i):
	return int(i/2)*2 == i

# get_caps_per_ticket(lib_type) -- determine caps per ticket
def caps_per_ticket(lib_type):
	if lib_type == '9310':
		return 10
	elif lib_type == 'aml2':
		return 7
	elif lib_type[:4] == '8500':
		return 5
	else:
		return None

def volumes_per_cap(lib_type):
	if lib_type == '9310':
		return 21
	elif lib_type == 'aml2':
		return 30
	elif lib_type[:4] == '8500':
		return 39
	else:
		return None

# same_tape_library(libs) -- check if all library are using the same robot
def same_tape_library(libs):
	l = libs.split(",")
	t = library_type(cluster, l[0])
	if len(l) > 1:
		for i in l[1:]:
			if library_type(cluster, i) != t:
				return None
	return t

# dump() -- dump all global variables
def dump():
	for i in __builtins__.globals().keys():
		if i[:2] == '__':	# internal
			continue
		if type(__builtins__.globals()[i]) == type(1) or \
			type(__builtins__.globals()[i]) == type(1.0) or \
			type(__builtins__.globals()[i]) == type("") or \
			type(__builtins__.globals()[i]) == type({}) or \
			type(__builtins__.globals()[i]) == type([]):
			print i, '=',
			pprint.pprint(__builtins__.globals()[i])

# complex operations

# CAPS_PER_TICKET = 10
# VOLUMES_PER_CAP = 21

def recommend_write_protect_job(library=DEFAULT_LIBRARIES, limit=None):
	# check if they are of the same robot
	lt = same_tape_library(library)
	if not lt:
		print "Error: %s are not the same robot"%(library)
		return {}

	CAPS_PER_TICKET = caps_per_ticket(lt)
	VOLUMES_PER_CAP = volumes_per_cap(lt)

	# take care of limit:
	# if limit == None: limit = default
	# if limit == 0: no limit
	# if limit == n, let it be n

	if limit == None:	# use default
		limit = CAPS_PER_TICKET * VOLUMES_PER_CAP

	if lt == 'aml2':
		op = 'aWP'
	elif lt == '8500GS':
		op = 'rWP'
	elif lt == '8500G1':
		op = 'sWP'
	elif lt == '8500F1':
		op = 'tWP'
	else:
		op = 'WP'
	# get max cap number
	n = get_max_cap_number(cluster, op) + 1
	# get exclusion list:
	q = "select object from object, job \
		where \
			object.job = job.id and \
			job.finish is null;"

	if debug:
		print q
	excl = db.query_getresult(q)

	# take care of libraries
	lb = library.split(",")
	lbs = "(library = '%s'"%(lb[0])
	for i in lb[1:]:
		lbs = lbs + " or library = '%s'"%(i)
	lbs = lbs+")"

	q = "" # to make lint happy
	if excl:
		exclusion = "'%s'"%(excl[0][0])
		for i in excl[1:]:
			exclusion = exclusion+','+"'%s'"%(i[0])
		q = "select label from volume where \
			%s and \
			system_inhibit_0 = 'none' and \
			system_inhibit_1 = 'full' and \
			write_protected != 'y' and \
			not storage_group in (select * from no_flipping_storage_group) and \
			not storage_group||'.'||file_family in \
			(select storage_group||'.'||file_family \
				from no_flipping_file_family) and\
			not file_family like '%%-MIGRATION%%' and \
			not label in (%s) \
			order by si_time_1 asc"%(lbs, exclusion)
	else:
		q = "select label from volume where \
			%s and \
			system_inhibit_0 = 'none' and \
			system_inhibit_1 = 'full' and \
			write_protected != 'y' and \
			not storage_group in (select * from no_flipping_storage_group) and \
			not storage_group||'.'||file_family in \
			(select storage_group||'.'||file_family \
				from no_flipping_file_family) and\
			not file_family like '%%-MIGRATION%%' \
			order by si_time_1 asc "%(lbs)
	if limit:
		q = q + ' limit %d;'%(limit)
	else:
		q = q + ';'

	if debug:
		print q
	res = edb.query_getresult(q)
	job = {}
	j = 0
	cap_n = n
	for i in range(len(res)):
		if j == 0:
			job[cap_n] = []
		job[cap_n].append(res[i][0])
		j = j + 1
		if j >= VOLUMES_PER_CAP:
			j = 0
			cap_n = cap_n + 1
	return job

def recommend_write_permit_job(library=DEFAULT_LIBRARIES, limit=None):
	# check if they are of the same robot
	lt = same_tape_library(library)
	if not lt:
		print "Error: %s are not the same robot"%(library)
		return {}

	CAPS_PER_TICKET = caps_per_ticket(lt)
	VOLUMES_PER_CAP = volumes_per_cap(lt)

	# take care of limit:
	# if limit == None: limit = default
	# if limit == 0: no limit
	# if limit == n, let it be n
	if limit == None:       # use default
		limit = CAPS_PER_TICKET * VOLUMES_PER_CAP

	if lt == 'aml2':
		op = 'aWE'
	elif lt == '8500GS':
		op = 'rWE'
	elif lt == '8500G1':
		op = 'sWE'
	elif lt == '8500F1':
		op = 'tWE'
	else:
		op = 'WE'

	# get max cap number
	n = get_max_cap_number(cluster, op) + 1
	# get exclusion list:
	q = "select object from object, job \
		where \
			object.job = job.id and \
			job.finish is null;"
	if debug:
		print q
	excl = db.query_getresult(q)

	# take care of libraries
	lb = library.split(",")
	lbs = "(library = '%s'"%(lb[0])
	for i in lb[1:]:
		lbs = lbs + " or library = '%s'"%(i)
	lbs = lbs+")"

	q = ""	# to make lint happy
	if excl:
		exclusion = "'%s'"%(excl[0][0])
		for i in excl[1:]:
			exclusion = exclusion+','+"'%s'"%(i[0])
		q = "select label from volume where \
			%s and \
			system_inhibit_0 = 'none' and \
			system_inhibit_1 = 'none' and \
			write_protected != 'n' and \
			not storage_group in (select * from no_flipping_storage_group) and \
			not file_family like '%%-MIGRATION%%' and \
			not storage_group||'.'||file_family in \
			(select storage_group||'.'||file_family \
				from no_flipping_file_family) and\
			not label in (%s) \
			order by label"%(lbs, exclusion)
	else:
		q = "select label from volume where \
			%s and \
			system_inhibit_0 = 'none' and \
			system_inhibit_1 = 'none' and \
			write_protected != 'n' and \
			not storage_group in (select * from no_flipping_storage_group) and \
			not storage_group||'.'||file_family in \
			(select storage_group||'.'||file_family \
				from no_flipping_file_family) and\
			not file_family like '%%-MIGRATION%%' \
			order by label "%(lbs)

	if limit:
		q = q + " limit %d;"%(limit)
	else:
		q = q + ";"

	if debug:
		print q
	res = edb.query_getresult(q)
	job = {}
	j = 0
	cap_n = n
	for i in range(len(res)):
		if j == 0:
			job[cap_n] = []
		job[cap_n].append(res[i][0])
		j = j + 1
		if j >= VOLUMES_PER_CAP:
			j = 0
			cap_n = cap_n + 1
	return job

# make_cap_args(d) -- make arguments from a dictionary
def make_cap_args(d):
	res = []
	for k in d.keys():
		if d[k]:
			res.append('CAP' + str(k) + ':')
			for i in d[k]:
				res.append(i)
	return res

# make_cap(list)
def make_cap(l, library_type='9310', cap_n = 0):
	cap_script = ""
	if library_type == '9310':
		if cluster == "D0":
			cap_script = "/usr/bin/rsh fntt -l acsss 'echo eject 1,0,0 "
		elif cluster == "STK":
			cap_script = "/usr/bin/rsh fntt -l acsss 'echo eject 0,0,0 "
		elif cluster == "CDF":
			cap_script = "/usr/bin/rsh fntt2 -l acsss 'echo eject 0,1,0 "
		else:
			return None
		for i in l:
			cap_script = cap_script + ' ' + i
		cap_script = cap_script + " \\\\r logoff|bin/cmd_proc -l -q 2>/dev/null'\n"
	elif library_type == 'aml2':
		cap_script = ''
		if cap_n % 2:	# odd
			door = ' E03\n'
		else:
			door = ' E06\n'
		count = 0
		for i in l:
			if count == 0:
				cap_script = cap_script + "dasadmin eject -t 3480 "+ i
			else:
				cap_script = cap_script +','+ i
			count = count + 1
			if count == 10:
				cap_script = cap_script + door
				count = 0
		if count != 0:
			cap_script = cap_script + door
	elif library_type == '8500GS':
		cap_script = "/usr/bin/rsh fntt -l acsss 'echo eject 2,1,0 "
		for i in l:
			cap_script = cap_script + ' ' + i
		cap_script = cap_script + " \\\\r logoff|bin/cmd_proc -l -q 2>/dev/null'\n"
	elif library_type == '8500G1':
		cap_script = "/usr/bin/rsh fntt-gcc -l acsss 'echo eject 0,5,0 "
		for i in l:
			cap_script = cap_script + ' ' + i
		cap_script = cap_script + " \\\\r logoff|bin/cmd_proc -l -q 2>/dev/null'\n"
	elif library_type == '8500F1':
		if cluster == "D0":
			cap_script = "/usr/bin/rsh fntt2 -l acsss 'echo eject 1,9,0 "
		elif cluster == "STK":
			cap_script = "/usr/bin/rsh fntt2 -l acsss 'echo eject 1,1,0 "
		elif cluster == "CDF":
			cap_script = "/usr/bin/rsh fntt2 -l acsss 'echo eject 1,5,0 "
		else:
			return None
		for i in l:
			cap_script = cap_script + ' ' + i
		cap_script = cap_script + " \\\\r logoff|bin/cmd_proc -l -q 2>/dev/null'\n"

	return cap_script

# get_max_cap_number(cluster)
def get_max_cap_number(cluster, op_type=''):
	q = "select max(to_number(substr(association, 4), 'FM999999')) \
		from object, job \
		where name like '%s%s%%' and object.job = job.id;"%(
		cluster, op_type)
	res = db.query_getresult(q)
	if res[0][0]:
		return int(res[0][0])
	else:
		return 0

def make_help_desk_ticket(n, cluster, script_host, job, library_type='9310'):
	if job == "protect":
		action = "lock"
	elif job == "permit":
		action = "unlock"
	else:
		action = "do not touch"
	VOLUMES_PER_CAP = volumes_per_cap(library_type)
	system_name = script_host

	short_message = "write %s %d tapes (flip tabs) in %s %s tape library"%(job, n, cluster.lower()+'en', library_type.upper())
	long_message = 'Please run "flip_tab %s" on %s to write %s %d tapes (%d caps) in %s enstore %s tape library.'%(action, script_host, job, n, int((n-1)/VOLUMES_PER_CAP)+1, cluster, library_type.upper())

	return snow_fliptab.submit_ticket(
		Summary=short_message,
		Comments=long_message,
		CiName = system_name.upper().split('.')[0],
		)

def get_last_job_time(cluster, job_type):
	q = "select max(start) from job, job_definition \
		where job_definition.name = '%s' and \
			job.type = job_definition.id and \
			job.name like '%s%%';"%(job_type, cluster)
	if debug:
		print q
	res = db.query_getresult(q)[0][0]
	if res:
		return timestamp2time(res.split('.')[0])
	return 0

def get_last_write_protect_on_job_time(l=None,c=None):
	if not c:
		c = cluster
	if l:
		lt = same_tape_library(l)
		if not lt:	# wrong cluster/library
			return -1
		q = get_qualifier(lt)
		if q:
			c = c+q
	return get_last_job_time(c, 'WRITE_PROTECTION_TAB_ON')

def get_last_write_protect_off_job_time(l=None, c=None):
	if not c:
		c = cluster
	if l:
		lt = same_tape_library(l)
		if not lt:      # wrong cluster/library
			return -1
		q = get_qualifier(lt)
		if q:
			c = c+q
	return get_last_job_time(c, 'WRITE_PROTECTION_TAB_OFF')

PROMPT = "operation> "

# shell() -- interactive shell
def shell():
	while True:
		sys.stdout.write(PROMPT)
		# handle "..."
		line = sys.stdin.readline()
		if line == '':
			print "quit"
			return
		elif line == '\n':
			continue
		parts = line.strip().split('"')
		args = []
		for i in range(len(parts)):
			if even(i):
				for j in parts[i].split():
					args.append(j)
			else:
				args.append(parts[i])
		if args and (args[0] == 'quit' or args[0] == 'exit'):
			break
		res = execute(args)
		if res:
			if type(res) == type([]):
				for i in res:
					print i
			else:
				print res
	return


# execute(args) -- execute args[0], args[1:]
def execute(args):
	n_args = len(args)
	if n_args < 1:
		return None

	cmd = args[0]

	if cmd == "dump": # dump all global variables
		dump()
		return
	elif cmd == "list": # list all job
		if n_args < 2 or args[1] == 'all':
			q = "select job.id, job.name, \
				job_definition.name as job, start, \
				finish, comment \
				from job, job_definition where \
					job.type = job_definition.id \
				order by job.start;"
			if debug:
				print q
			return db.query(q)
		elif args[1] == 'open':
			q = "select job.id, job.name, \
				job_definition.name as job, start, \
				finish, comment \
				from job, job_definition where \
					job.type = job_definition.id and \
					finish is null \
				order by job.start;"
			if debug:
				print q
			return db.query(q)
		elif args[1] == 'closed' or args[1] == 'completed' or args[1] == 'finished':
			q = "select job.id, job.name, \
				job_definition.name as job, start, \
				finish, comment \
				from job, job_definition where \
					job.type = job_definition.id and \
					not finish is null \
				order by job.start;"
			if debug:
				print q
			return db.query(q)
		elif args[1] == 'has':
			qq = "select job.id, job.name, \
				job_definition.name as job, start, \
				finish, comment \
				from job, job_definition, object where \
					job.type = job_definition.id \
					and object.job = job.id \
					and object.object = '%s'"
			q = qq%(args[2])
			for i in args[2:]:
				q =  q + " intersect (%s)"%(qq%(i))
			q = q + ";"
			if debug:
				print q
			return db.query(q)
		elif args[1] == 'recent':
			if len(args) > 2:
				limit = int(args[2])
			else:
				limit = 20
			q = "select job.id, job.name, \
				job_definition.name as job, start, \
				finish, comment \
				from job, job_definition where \
					job.type = job_definition.id \
				order by job.start desc limit %d;"%(
				limit)
			if debug:
				print q
			return db.query(q)
		else:
			or_stmt = "job.name like '%s' "%(args[1])
			for i in args[2:]:
				or_stmt = or_stmt + "or job.name like '%s' "%(i)
			q = "select job.id, job.name, \
				job_definition.name as job, start, \
				finish, comment \
				from job, job_definition where \
					job.type = job_definition.id \
					and (%s) \
				order by job.start;"%(or_stmt)
			if debug:
				print q
			return db.query(q)
	elif cmd == "show": # show a job
		for i in args[1:]:
			job = get_job_by_name(i)
			# pprint.pprint(job)
			show(job)
	elif cmd == "create": # create job
		if args[1] == "write_protect_on":
			return create_write_protect_on_job(args[2], args[3:])
		elif args[1] == "write_protect_off":
			return create_write_protect_off_job(args[2], args[3:])
		else:
			return "don't know what to do"
	elif cmd == "auto_write_protect_on":
		if len(args) > 2:
			if args[2] == "no_limit":
				res = recommend_write_protect_job(args[1], limit=0)
			else:
				res = recommend_write_protect_job(args[1])
		elif len(args) == 2:
			if args[1]  == "no_limit":
				res = recommend_write_protect_job(limit=0)
			else:
				res = recommend_write_protect_job(args[1])
		else:
			res = recommend_write_protect_job()

		# create job
		if res:
			# get the qualifier
			if len(args) > 1:
				lt = library_type(cluster,args[1].split(',')[0])
			else:
				lt = library_type(cluster, get_default_library(cluster).split(',')[0])
			qf = get_qualifier(lt)

			job_name = cluster+qf+'WP'+`min(res.keys())`+'-'+`max(res.keys())`
			create_write_protect_on_job(job_name, make_cap_args(res), 'AUTO-GENERATED')
			# clean up temp directory
			clean_up_temp_dir()
			total = 0
			for i in res.keys():
				total = total + len(res[i])
				f = open(os.path.join(TEMP_DIR, str(i)), 'w')
				f.write(make_cap(res[i], lt, i))
				f.close()
			cc = "cd %s; enrcp * %s:%s"%(TEMP_DIR, script_host,
				get_write_protect_script_path(lt))
			print cc
			os.system(cc)
			ticket = make_help_desk_ticket(total, cluster, script_host, 'protect', lt)
			print "ticket =", ticket
			res2 = start_next_task(job_name, ticket)
			res2.append(show_current_task(job_name))
			res2.append("ticket = "+ticket)
			return res2
		else:
			return "no more volumes to do"
	elif cmd == "auto_write_protect_off":
		if len(args) > 2:
			if args[2] == "no_limit":
				res = recommend_write_permit_job(args[1], limit=0)
			else:
				res = recommend_write_permit_job(args[1])
		elif len(args) == 2:
			if args[1]  == "no_limit":
				res = recommend_write_permit_job(limit=0)
			else:
				res = recommend_write_permit_job(args[1])
		else:
			res = recommend_write_permit_job()
		if res:
			# create job
			if len(args) > 1:
				lt = library_type(cluster,args[1].split(',')[0])
			else:
				lt = library_type(cluster, get_default_library(cluster).split(',')[0])
			qf = get_qualifier(lt)

			job_name = cluster+qf+'WE'+`min(res.keys())`+'-'+`max(res.keys())`
			create_write_protect_off_job(job_name, make_cap_args(res), 'AUTO-GENERATED')
			# clean up temp directory
			clean_up_temp_dir()
			total = 0
			for i in res.keys():
				total = total + len(res[i])
				f = open(os.path.join(TEMP_DIR, str(i)), 'w')
				f.write(make_cap(res[i], lt, i))
				f.close()
			cc = "cd %s; enrcp * %s:%s"%(TEMP_DIR, script_host,
				get_write_permit_script_path(lt))
			print cc
			os.system(cc)
			ticket = make_help_desk_ticket(total, cluster, script_host, 'permit', lt)
			print "ticket =", ticket
			res2 = start_next_task(job_name, ticket)
			res2.append(show_current_task(job_name))
			res2.append("ticket = "+ticket)
			return res2
		else:
			return "no more volumes to do"
	elif cmd == "recommend_write_protect_on":
		if len(args) > 3:
			if args[2] == 'limit':
				res = recommend_write_protect_job(library = args[1], limit=int(args[3]))
			else:
				res = recommend_write_protect_job(library = args[1], limit=0)
		elif len(args) == 3:
			if args[1] == 'limit':
				res = recommend_write_protect_job(limit=int(args[2]))
			else:
				res = recommend_write_protect_job(limit=0)
		elif len(args) == 2:
			res = recommend_write_protect_job(library = args[1], limit=0)
		else:
			res = recommend_write_protect_job(limit=0)
		# pprint.pprint(res)
		for i in res:
			show_cap(i, res[i])
		total = 0
		caps = len(res)
		for i in res.keys():
			total = total + len(res[i])
		print "%d tapes in %d caps"%(total, caps)
		return ""
	elif cmd == "recommend_write_protect_off":
		if len(args) > 3:
			if args[2] == 'limit':
				res = recommend_write_permit_job(library = args[1], limit=int(args[3]))
			else:
				res = recommend_write_permit_job(library = args[1], limit=0)
		elif len(args) == 3:
			if args[1] == 'limit':
				res = recommend_write_permit_job(limit=int(args[2]))
			else:
				res = recommend_write_permit_job(limit=0)
		elif len(args) == 2:
			res = recommend_write_permit_job(library = args[1], limit=0)
		else:
			res = recommend_write_permit_job(limit=0)
		# pprint.pprint(res)
		for i in res:
			show_cap(i, res[i])
		total = 0
		caps = len(res)
		for i in res.keys():
			total = total + len(res[i])
		print "%d tapes in %d caps"%(total, caps)
		return ""
	elif cmd == "current": # current task
		result = []
		for i in args[1:]:
			result.append(show_current_task(i))
		return result
	elif cmd == "next": # next task
		result = []
		for i in args[1:]:
			result.append(show_next_task(i))
		return result
	elif cmd == "start":
		timestamp = None
		arg = None
		comment = None
		if n_args < 2:
			return "which job?"
		job = args[1]
		if n_args > 2:
			if is_time(args[2]):
				timestamp = args[2].replace('_', ' ')
				if n_args > 3:
					arg = args[3]
				if n_args > 4:
					comment = args[4]
			else:
				arg = args[2]
				if n_args > 3:
					comment = args[3]
		res = start_next_task(args[1], arg, comment, timestamp)
		res.append(show_current_task(args[1]))
		return res
	elif cmd == "finish":
		timestamp = None
		result = None
		comment = None
		if n_args < 2:
			return "which job?"
		if n_args > 2:
			if is_time(args[2]):
				timestamp = args[2].replace('_', ' ')
				if n_args > 3:
					result = args[3]
				if n_args > 4:
					comment = args[4]
			else:
				result = args[2]
				if n_args > 3:
					comment = args[3]
		res = finish_current_task(args[1], result, comment, timestamp)
		res.append(show_current_task(args[1]))
		return res
	elif cmd == "delete":
		if args[-1] != "sincerely":
			print "If you really want to delete the job(s), you have to say:"
			for i in args:
				print i,
			print "sincerely"
		else:
			for i in args[1:-1]:
				print "deleting job %s ..."%(i),
				delete(i)
				print "done"
	elif cmd == "find" or cmd == "locate":
		for i in args[1:]:
			print "Jobs that %s is in:"%(i)
			q = "select job.id, job.name, \
				job_definition.name as job, start, \
				finish, comment \
				from job, job_definition, object \
				where \
					job.type = job_definition.id \
					and \
					object.job = job.id and \
					object.object = '%s' \
				order by job.start;"%(i)
			if debug:
				print q
			print db.query(q)
	elif cmd == "find+" or cmd == "locate+":	# with details
		for i in args[1:]:
			print "Jobs that %s is in:"%(i)
			q = "select job.name, job.start \
				from job, object \
				where \
					object.job = job.id and \
					object.object = '%s' \
				order by job.start;"%(i)
			if debug:
				print q
			res =  db.query_getresult(q)
			for j in res:
				job = get_job_by_name(j[0])
				show(job)
	elif cmd == "relate":
		for i in args[1:]:
			print "Job(s) that is(are) related to %s"%(i)
			q = "select job.id, job.name, \
				job_definition.name as job, start, \
				finish, comment \
				from job, job_definition where \
					job.type = job_definition.id and \
					job.name in ( \
						select distinct j2.name \
						from job j1, job j2, object o1, object o2 \
						where \
							j1.id <= j2.id and \
							j1.id = o1.job and \
							j2.id = o2.job and \
							o1.object = o2.object and \
							j1.name = '%s') \
				order by start;"%(i)
			if debug:
				print q
			print db.query(q)
	elif cmd == "auto_close_all":
		auto_close_all()
	elif cmd == "help":
		if len(args) == 1:
			help()
		else:
			help(args[1])
	else:
		return 'unknown command "%s"'%(cmd)



if __name__ == "__main__":

	if len(sys.argv) < 2:
		shell()
	else:
		res = execute(sys.argv[1:])
		if res:
			if type(res) == type([]):
				for i in res:
					print i
			else:
				print res
