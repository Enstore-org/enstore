#!/usr/bin/env python
'''
fileinfo.py -- get enstore file info of a file specified in /pnfs path
as well as the volume info associated with it.
'''

import option
import file_clerk_client
import volume_clerk_client
import sys
import os
import string
import pprint
import e_errors

fcc = None
vcc = None

# layer_file(p, n): get layer n file path from the path p
def layer_file(p, n):
	d, f = os.path.split(p)
	return os.path.join(d, '.(use)(%d)(%s)'%(n, f))

def file_info(path):
	if not os.access(path, os.R_OK):
		return None
	# try to get bfid
	l4 = map(string.strip, open(layer_file(path, 4)).readlines())
	if len(l4) < 9:
		return None
	bfid = l4[8]

	fi = fcc.bfid_info(bfid)
	if fi['status'][0] != e_errors.OK:
		return None
	vi = vcc.inquire_vol(fi['external_label'])
	if vi['status'][0] != e_errors.OK:
		return None

	return {'file_info':fi, 'volume_info': vi}

if __name__ == '__main__':
	if len(sys.argv) < 2:
		print 'None'
		sys.exit(0)
	intf = option.Interface()
	fcc = file_clerk_client.FileClient((intf.config_host, intf.config_port))
	vcc = volume_clerk_client.VolumeClerkClient(fcc.csc)

	result = {}
	for i in sys.argv[1:]:
		out = file_info(i)
		result[i] = out
	pprint.pprint(result)
