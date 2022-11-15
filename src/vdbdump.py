#!/usr/bin/env python

import db
import sys
import time
import string
import pprint

# time2timestamp(t) -- convert time to "YYYY-MM-DD HH:MM:SS"
def time2timestamp(t):
	return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t))

# timestamp2time(ts) -- convert "YYYY-MM-DD HH:MM:SS" to time 
def timestamp2time(s):
	return time.mktime(time.strptime(s, "%Y-%m-%d %H:%M:%S"))

def formatedv(vol):
	blocksize = vol['blocksize']
	capacity_bytes = vol['capacity_bytes']
	declared = time2timestamp(vol['declared'])
	eod_cookie = vol['eod_cookie']
	external_label = vol['external_label']
	first_access = time2timestamp(vol['first_access'])
	last_access = time2timestamp(vol['last_access'])
	library = vol['library']
	media_type = vol['media_type']
	non_del_files = vol['non_del_files']
	remaining_bytes = vol['remaining_bytes']
	if vol.has_key('sum_mounts'):
		sum_mounts = vol['sum_mounts']
	else:
		sum_mounts = 0
	sum_rd_access = vol['sum_rd_access']
	sum_rd_err = vol['sum_rd_err']
	sum_wr_access = vol['sum_wr_access']
	sum_wr_err = vol['sum_wr_err']
	system_inhibit_0 = vol['system_inhibit'][0]
	system_inhibit_1 = vol['system_inhibit'][1]
	if vol.has_key('si_time'):
		si_time_0 = time2timestamp(vol['si_time'][0])
		si_time_1 = time2timestamp(vol['si_time'][1])
	else:
		si_time_0 = declared
		si_time_1 = declared
	user_inhibit_0 = vol['user_inhibit'][0]
	user_inhibit_1 = vol['user_inhibit'][1]
	t = string.split(vol['volume_family'], '.')
	storage_group = t[0]
	file_family = t[1]
	if len(t) > 2:
		wrapper = t[2]
	else:
		wrapper = 'none'
	if vol.has_key('comment'):
		comment = vol['comment']
	else:
		comment = ''

	res = "%d\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s"%(
		blocksize, capacity_bytes, declared, eod_cookie,
		external_label, first_access, last_access, library,
		media_type, non_del_files, remaining_bytes, sum_mounts,
		sum_rd_access, sum_rd_err, sum_wr_access, sum_wr_err,
		system_inhibit_0, system_inhibit_1,
		si_time_0, si_time_1,
		user_inhibit_0, user_inhibit_1,
		storage_group, file_family, wrapper, comment)
	
	return res

if __name__ == "__main__":   # pragma: no cover
	vol = db.DbTable('volume', '.', '/tmp', [], 0)
	c = vol.newCursor()
	k, v = c.first()
	count = 0
	if len(sys.argv) > 1:
		outf = open(sys.argv[1], 'w')
	else:
		outf = open('vol.dmp', 'w')

	last_time = time.time()
	while k:
		l = formatedv(v)
		outf.write(l+'\n')
		k, v = c.next()
		count = count + 1
		if count % 1000 == 0:
			time_now = time.time()
			print "%12d %14.2f records/sec"%(count,
				1000.0/(time_now - last_time))
			last_time = time_now
		# if count > 10:
		#	break
	outf.close()
	c.close()
	vol.close()
