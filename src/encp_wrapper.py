#!/usr/bin/env python
"""
a quick and not so dirty wrapper for encp so that multiple encps can be
instantiated in the same program, probably in different threads.
"""

import string
import sys

class Encp:
	def __init__(self, tid=None):
		import encp
		self.my_encp = encp
		self.tid = tid
		self.exit_status = -10

	# encp(cmd) -- cmd is the same as the command line
	# eg. encp("encp --verbose=4 /pnfs/.../file file")
	def encp(self, cmd):
		self.exit_status = -10 #Reset this every time.
		
		if cmd[:4] != "encp":
			cmd = "encp "+cmd
		argv = string.split(cmd)
		intf = self.my_encp.EncpInterface(argv, 0)
		intf.migration_or_duplication = 1 #Set true for performance.
		if self.tid:
			intf.include_thread_name = self.tid
		try:
			res = self.my_encp.do_work(intf)
			if res == None:
				#return -10
				res = -10  #Same as initial value.
		except:
			res = 1

		self.exit_status = res #Return value if used in a thread.
		return res  #Return value if used directly.

	

if __name__ == '__main__':
	test_encp = Encp()
	for i in sys.argv[1:]:
		print "copying", i, "...",
		cmd = "encp --priority 0 --ignore-fair-share %s /dev/null"%(i)
		res = test_encp.encp(cmd)
		if res:
			print "FAILED"
		else:
			print "DONE"
