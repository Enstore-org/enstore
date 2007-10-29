#!/usr/bin/env python
"""
ecron_util.py -- utility routines for ecron
"""

import os
import option
import configuration_client
import e_errors
import pg
import popen2
import time

gscript = """
set term postscript landscape enhanced color solid 'Helvetica' 10
set output '%s.ps'
set xlabel 'Date'
set timefmt '%%Y-%%m-%%d:%%H:%%M:%%S'
set xdata time
set xrange ['%s' :'%s']
set yrange [-5:15]
set key box
set grid
set label ' DIS ' at graph 0,.2 right
set label ' ACT ' at graph 0,.15 right
set label ' OOB ' at graph 0,.6 right
plot '-' using 1:2 t '%s' with points 13, 0 t '' with lines 2, 10 t '' with lines 2, '-' using 1:2 t 'Last Monday' with lines lt 3 lw 4
"""

# show past time

def ago(year, mon, day):
	t = time.localtime(time.time())
	return time.strftime("%Y-%m-%d:%H:%M:%S", time.localtime(time.mktime((t[0] - year, t[1] - mon, t[2] - day, 0, 0, 0, 0, 0, 1))))

def today():
	return ago(0, 0, 0)

def yesterday():
	return ago(0, 0, 1)

def tomorrow():
	return ago(0, 0, -1)

def one_week_ago():
	return ago(0, 0, 7)

def month_ago(mon):
	return ago(0, mon, 0)

def one_month_ago():
	return month_ago(1)

def six_month_ago():
	return month_ago(6)

def year_ago(year):
	return ago(year, 0, 0)

def one_year_ago():
	return year_ago(1)

# remember mon=0, tue=1, ..., sun=6
def last_weekday(d):
	if d < 0:
		d = 0
	if d > 6:
		d = 6
	t = time.localtime(time.time())
	tt = t[6] - d 
	if tt <= 0:
		tt = tt + 7
	return time.strftime("%Y-%m-%d:%H:%M:%S", time.localtime(time.mktime((t[0], t[1], t[2] - tt, 0, 0, 0, 0, 0, 1))))

def last_monday():
	return last_weekday(0)

# odd duration

duration = {
'log-stash': six_month_ago,
'backupSystem2Tape': one_month_ago}

# backward compatibile
OLD_CRONS_DIR = '/diska/CRONS'

class EcronData:
	# get database connection
	def __init__(self):
		intf = option.Interface()
		csc = configuration_client.ConfigurationClient((intf.config_host, intf.config_port))
		acs = csc.get('accounting_server')
		self.db = pg.DB(host=acs['dbhost'], port=acs['dbport'], dbname=acs['dbname'])
		web_server = csc.get('web_server')
		if web_server['status'][0] == e_errors.OK:
			self.crons_dir = os.path.join(web_server['DocumentRoot'], 'enstore', 'CRONS')
		else:
			self.crons_dir = OLD_CRONS_DIR

		self.all_names = None
		self.all_names_and_nodes = None

	# list of cron
	def get_all_names(self):
		if self.all_names == None:
			q = "select distinct name from event;"
			res = self.db.query(q).getresult()
			self.all_names = []
			for i in res:
				self.all_names.append(i[0])
		return self.all_names

	def get_all_names_and_nodes(self):
		if self.all_names_and_nodes == None:
			q = "select distinct name, node from event;"
			res = self.db.query(q).getresult()
			self.all_names_and_nodes = []
			for i in res:
				self.all_names_and_nodes.append(i)
		return self.all_names_and_nodes

	# get_cron_result() -- get result just like old cronHISTOGRAM
	def get_result(self, name, node, start=None):
		if start:
			q = "(select start as time, 10 from event where name = '%s' and node = '%s' and start >= '%s') union (select finish as time, status from event where name = '%s' and node = '%s' and not finish is null and start >= '%s') order by time;"%(name, node, start, name, node, start)
		else:
			q = "(select start as time, 10 from event where name = '%s' and node = '%s') union (select finish as time, status from event where name = '%s' and node = '%s' and not finish is null) order by time;"%(name, node, name, node)
		return self.db.query(q).getresult()

	def plot(self, file, name, data):
		# get a gnuplot
		# need to put convert together here to make sure the files
		# are ready before the conversion
		(out, gp) = popen2.popen2("gnuplot; convert -rotate 90 %s.ps %s.jpg; convert -rotate 90 -geometry 120x120 -modulate -20 %s.ps %s_stamp.jpg"%(file,file,file,file))
		gp.write(gscript%(file, self.get_duration(name), tomorrow(), name))
		if not data:
			gp.write("%s -5\n"%(one_year_ago()))
		else:
			for i in data:
				gp.write("%s %d\n"%(i[0].replace(' ', ':'), i[1]))
		gp.write("e\n")
		gp.write("%s, %d\n"%(last_monday(), -3))
		gp.write("%s, %d\n"%(last_monday(), -5))
		gp.write("e\n")
		gp.write("quit\n")
		gp.close()

	def get_duration(self, name):
		if name in duration.keys():
			return duration[name]()
		else:
			return one_week_ago()

if __name__ == '__main__':
	ecrond = EcronData()
	names_and_nodes = ecrond.get_all_names_and_nodes()
	for i in names_and_nodes:
		print "name =", i[0], ", node =", i[1]
