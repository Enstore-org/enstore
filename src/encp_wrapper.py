#!/usr/bin/env python
"""
a quick and not so dirty wrapper for encp so that multiple encps can be
instantiated in the same program, probably in different threads.
"""

import encp
import string
import sys
import e_errors

class Encp:
	def __init__(self):
		import encp
		self.my_encp = encp

	# encp(cmd) -- cmd is the same as the command line
	# eg. encp("encp --verbose=4 /pnfs/.../file file")
	def encp(self, cmd):
		if cmd[:4] != "encp":
			cmd = "encp "+cmd
		argv = string.split(cmd)
		intf = self.my_encp.EncpInterface(argv, 0)
		try:
			return self.my_encp.main(intf)
		except:
			return 1

if __name__ == '__main__':
	encp = Encp()
	for i in sys.argv[1:]:
		print "copying", i, "...",
		cmd = "encp --priority 0 --ignore-fair-share %s /dev/null"%(i)
		res = encp.encp(cmd)
		if res:
			print "FAILED"
		else:
			print "DONE"
