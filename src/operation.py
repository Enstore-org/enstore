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

import pg
import time
import pprint
import types
import sys

# enstore import
import option
import configuration_client

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

DATABASEHOST = 'stkensrv6.fnal.gov'
# DATABASEHOST = 'localhost'
DATABASEPORT = 5432;
DATABASENAME = 'operation'

intf = option.Interface()
csc = configuration_client.ConfigurationClient((intf.config_host, intf.config_port))
enstoredb = csc.get('database')

# get_db() -- initialize a database connection
def get_db():
	return pg.DB(dbname = DATABASENAME, port = DATABASEPORT, host = DATABASEHOST)

def get_edb(enstoredb):
	return pg.DB(host=enstoredb['db_host'], port=enstoredb['db_port'], dbname=enstoredb['dbname'])

# global db connection
db = get_db()
edb = get_edb(enstoredb)

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
				job.type = job_definition.id;"%(i)
		res = db.query(q).dictresult()
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

	db.query(q)
	# get job id
	q = "select id from job where name = '%s';"%(
		name)
	id = db.query(q).getresult()[0][0]
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
		# print q
		db.query(q)
	return id

# get_job_by_name() -- from a name to find the job; name is unique
def get_job_by_name(name):
	q = "select * from job where name = '%s';"%(name)
	res = db.query(q).dictresult()
	if res:
		return retrieve_job(res[0])
	else:
		return None

# get_job_by_id() -- get_job_using internal id
def get_job_by_id(id):
	q = "select * from job where id = %d;"%(id)
	res = db.query(q).dictresult()
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
	res = db.query(q).dictresult()
	if res:
		return retrieve_job(res[0])
	else:
		return None

# retrieve_job() -- get all related information of this job
def retrieve_job(job):
	# assemble its related objects
	q = "select * from object where job = %d order by association, object;"%(job['id'])
	object = []
	res = db.query(q).getresult()
	for j in res:
		if j[2]:
			object.append(j[2]+':'+j[1])
		else:
			object.append(j[1])
	job['object'] = object
	# list its related tasks
	q = "select * from job_definition where id = %d;"%(job['type'])
	job_definition = db.query(q).dictresult()[0]
	job['job_definition'] = job_definition
	q = "select * from task left outer join progress \
		on (progress.job = %d and task.seq = progress.task) \
		where task.job_type = %d \
			order by seq;"%(job['id'], job['type'])
	job['task'] = db.query(q).dictresult()
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
	return db.query(q).dictresult()

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
	res = db.query(q)

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
	res = db.query(q)

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
	res = db.query(q).getresult()
	return res[0][0]

# get_next_task(name) -- get next task
def get_next_task(name):
	q = "select tasks, finish from job, job_definition where \
		job.name = '%s' and \
		job.type = job_definition.id;"%(name)
	res = db.query(q).getresult()
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
	res = db.query(q).getresult()
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
	res = db.query(q).getresult()
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
			start_job_task(job, nt, args, comment, timestamp)
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
			finish_job_task(job, ct, comment, result, timestamp)
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
	ct = db.query(q).dictresult()[0]
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
	ct = db.query(q).dictresult()[0]
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
		print "%3d %s %40s (%s) %s %s %s %s"%(
			t['seq'], t['auto_start'], t['dsc'],
			t['action'], t['start'], t['finish'], t['args'],
			t['result'])
	print "Objects:"
	for i in job['object']:
		print "\t", i

# delete(job) -- delete a job
def delete(job):
	if job:
		q = "delete from job where name = '%s';"%(job)
		db.query(q)

def create_write_protect_on_job(name, args, comment = ''):
	return create_job(name, 'WRITE_PROTECTION_TAB_ON', args, comment)

def create_write_protect_off_job(name, args, comment = ''):
	return create_job(name, 'WRITE_PROTECTION_TAB_OFF', args, comment)

# help(topic) -- help function
def help(topic=None):
	if not topic:
		print "operation.py create write_protect_on|write_protect_off <job> [[<association>:] [<associate>:]<object>]+"
		print "    -- creating a job"
		print "operation.py list [all|open|finished|closed|completed|<job>+|has <object>]"
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
		print
		print "try:"
		print "operation.py help <topic>"
	elif topic == "create":
		print "operation.py create write_protect_on|write_protect_off <job> [[<association>:] [<associate>:]<object>]+"
		print
		print "<job> is a user defined unique name of the job"
		print "<association> is a way to group objects. By default, there is no assoication"
		print "<association>: change the association for the rest of the objects in list"
		print "     It is allowed to have multiple <association>: in the command."
		print "     Each <association>: changes the global association setting."
		print "<association>:<object> temporarily override default association for <object>"
		print
		print "EXAMPLE:"
		print "operation.py create write_protect_on WP3 CAP3:  VO2093 VO2094 VO2095 VO2096 VO2097 VO2098 VO2099 VO2152 VO2154 VO2195 VO2196 VO2197 VO2198 VO2199 VO2203 VO2206 VO2207 VO2208 VO2209 VO2211 VO2213 CAP4: VO2224 VO2225 VO2226 VO2227 VO2245 VO2246 VO2252 VO2253 VO2254 VO2256 VO2257 VO2258 VO2259 VO2501 VO2532 VO2533 VO2534 VO2540 VO2541 VO2542 VO2544"
	elif topic == "list":
		print "operation.py list [all|open|finished|closed|completed|<job>+|has <object>]"
		print
		print "all: list all jobs"
		print "open: list all open (not closed) jobs"
		print "finished|close|completed: list all completed jobs. finished|close|completed are the same thing"
		print "<job>+ : list named jobs"
		print "has <object>: list all jobs that have <object> as an argument"
	elif topic == "show":
		print "operation.py show <job>+"
		print
		print "show details of <job>s in the list. <job> is addressed by its unique name"
	elif topic == "current":
		print "operation.py current <job>+"
		print
		print "show the current task of <job>."
		print "A current task is one that has stared but its next task has not started"
		print "A job can have at most one such task at any time"
		print "in case of a not yet started job, current task is task 0"
		print "in case of a finished job, current task is the last task"
	elif topic == "next":
		print "operation.py next <job>+"
		print
		print "show next task of <job>"
		print "next task is one that has not started and its previous task has finished."
		print "in case of a have-not-started job, next task is the first task."
		print "in case of a finished job, next task is task 0"
	elif topic == "start":
		print "operation.py start <job> [<arg>]"
		print
		print "start the next task of <job> with optional argument"
		print "next task can start only if current task has finished"
		print
		print "EXAMPLE:"
		print "operation.py start STKWP3 <help_desk_ticket_id>"
	elif topic == "finish":
		print "operation.py finish <job> [<result>]"
		print
		print "finish current task of <job> with optional <result>"
		print "EXAMPLE:"
		print "operation.py finish STKWP3 DONE"
	elif topic == "delete":
		print "operation.py delete <job>+ [sincerely]"
		print
		print "delete <job> in the list"
		print "this is a dangerous command, use with extra care"
		print '<job>s will not be deleted unless "sincerely" is specified in the end'
	elif topic == "find" or topic == "locate":
		print "operation.py find|locate <object>+"
		print
		print "list the jobs that have <object> as an argument"
	elif topic == "find+" or topic == "locate+":
		print "operation.py find+|locate+ <object>+"
		print
		print "same as find|locate but show details of the jobs"
	else:
		print "don't know about %s"%(topic)
		print
		help()

# even(i) -- True is i is an even number
def even(i):
	return int(i/2)*2 == i

# complex operations

def recommend_write_protect_job(media_type, cap):
	# how many?
	n = len(cap)*21
	# get exclusion list:
	q = "select object from object, job \
		where \
			object.job = job.id and \
			job.finish is null;"
	excl = db.query(q).getresult()
	if excl:
		exclusion = "'%s'"%(excl[0][0])
		for i in excl[1:]:
			exclusion = exclusion+','+"'%s'"%(i[0])
		q = "select label from volume where \
			media_type = '%s' and \
			system_inhibit_0 = 'none' and \
			system_inhibit_1 = 'full' and \
			write_protected = 'n' and \
			not label in (%s) \
			order by label \
			limit %d;"%(media_type, exclusion, n)
	else:
		q = "select label from volume where \
			media_type = '%s' and \
			system_inhibit_0 = 'none' and \
			system_inhibit_1 = 'full' and \
			write_protected = 'n' and \
			order by label \
			limit %d;"%(media_type, n)
	res = edb.query(q).getresult()
	job = {}
	for i in cap:
		job[i] = []
	cp = 0
	for i in job.keys():
		for j in range(21):
			try:
				job[i].append(res[cp][0])
				cp = cp + 1
			except:
				break
		if j < 21: # early quit
			break
	return job

def recommend_write_permit_job(media_type, cap):
	# how many?
	n = len(cap)*21
	# get exclusion list:
	q = "select object from object, job \
		where \
			object.job = job.id and \
			job.finish is null;"
	excl = db.query(q).getresult()
	if excl:
		exclusion = "'%s'"%(excl[0][0])
		for i in excl[1:]:
			exclusion = exclusion+','+"'%s'"%(i[0])
		q = "select label from volume where \
			media_type = '%s' and \
			system_inhibit_0 = 'none' and \
			system_inhibit_1 = 'none' and \
			write_protected = 'y' and \
			not label in (%s) \
			order by label \
			limit %d;"%(media_type, exclusion, n)
	else:
		q = "select label from volume where \
			media_type = '%s' and \
			system_inhibit_0 = 'none' and \
			system_inhibit_1 = 'none' and \
			write_protected = 'y' and \
			order by label \
			limit %d;"%(media_type, n)
	res = edb.query(q).getresult()
	job = {}
	for i in cap:
		job[i] = []
	cp = 0
	for i in job.keys():
		for j in range(21):
			try:
				job[i].append(res[cp][0])
				cp = cp + 1
			except:
				break
		if j < 21: # early quit
			break
	return job

			

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

	if cmd == "list": # list all job
		if n_args < 2 or args[1] == 'all':
			q = "select job.id, job.name, \
				job_definition.name as job, start, \
				finish, comment \
				from job, job_definition where \
					job.type = job_definition.id \
				order by job.start;"
			return db.query(q)
		elif args[1] == 'open':
			q = "select job.id, job.name, \
				job_definition.name as job, start, \
				finish, comment \
				from job, job_definition where \
					job.type = job_definition.id and \
					finish is null \
				order by job.start;"
			return db.query(q)
		elif args[1] == 'closed' or args[1] == 'completed' or args[1] == 'finished':
			q = "select job.id, job.name, \
				job_definition.name as job, start, \
				finish, comment \
				from job, job_definition where \
					job.type = job_definition.id and \
					not finish is null \
				order by job.start;"
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
			return db.query(q)
		else:
			or_stmt = "job.name = '%s' "%(args[1])
			for i in args[2:]:
				or_stmt = or_stmt + "or job.name = '%s' "%(i)
			q = "select job.id, job.name, \
				job_definition.name as job, start, \
				finish, comment \
				from job, job_definition where \
					job.type = job_definition.id \
					and (%s) \
				order by job.start;"%(or_stmt)
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
			print "delete",
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
			res =  db.query(q).getresult()
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
			print db.query(q)
	elif cmd == "help":
		if len(args) == 1:
			help()
		else:
			help(args[1])
	else:
		return 'unknown command "%s"'%(cmd)


if __name__ == '__main__':

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
