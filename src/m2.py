# m2.py -- collection of mammoth-2 related routines

import os
import string
import enmail
import time
import getpass

# dump_code(device, path=None, sendto=None, notify=None, comment=None)
#
#	calling m2probe to dump the internal code of a m2 drive
#
#	device: the device that connects to a mammoth-2 drive
#	path:	path to the dump file, default to be CWD
#	sendto:	a list of email addresses to send the code dump to
#	notify:	a list of email addresses to send the notification to
#	comment:extra message that goes into notification
#		This is useful to pass enstore mover information

def dump_code(device, path=None, sendto=None, notify=None, comment=None):

	# use prefix to fake the path
	if path:
		prefix = '-p '+os.path.join(path, 'Fermilab')
	else:
		prefix = ''

	cmd = "m2probe -d %s %s"%(prefix, device)

	# parse m2probe's output for file name and status
	l = os.popen(cmd).readlines()
	res = string.split(l[-1], "dumped to")
	if len(res) != 2:	# something is wrong
		return 1

	status = string.join(l, '')
	# get the file name
	f = string.strip(res[-1])

	from_add = getpass.getuser()+'@'+os.uname()[1]
	subject = "M2 dump taken at "+time.ctime(time.time())

	# send it to some one?
	if sendto:
		mesg = "This is an automatically generated M2 dump by an enstore mover\n\n"+status
		enmail.mail_bin(from_add, sendto, subject, f, mesg)

	if notify:
		mesg = "A M2 dump is taken by "+from_add+"\n\n"+status
		if comment:
			mesg = mesg+"\n\n"+comment
		if sendto:
			if type(sendto) == type([]):
				to_addresses = string.joinfields(sendto, ', ')
			else:
				to_addresses = sendto
			mesg = mesg+"\n\nThe dump file has been sent to "+to_addresses
		enmail.mail(from_add, sendto, subject, mesg)

	return(0)
