#!/usr/bin/env python
"""
a quick and not so dirty wrapper for encp so that multiple encps can be
instantiated in the same program, probably in different threads.
"""

import sys
import thread

import Trace
import file_utils

class Encp:
	def __init__(self, tid=None):
		import encp
		self.my_encp = encp
		self.tid = tid
		self.exit_status = -10
		self.err_msg = ""

	# encp(argv) -- argv is the same as the command line
	# eg. encp(["encp", "--verbose=4", "/pnfs/.../file", "file"])
	def encp(self, argv):
		self.exit_status = -10 #Reset this every time.
		self.err_msg = ""

		#Insert the command if it is not already there.
                CMD = "encp"
		if argv[0] != CMD:
			argv = [CMD] + argv
		
		try:
			logname=Trace.logname #Grab this to reset this after encp.
			intf = self.my_encp.EncpInterface(argv, 0)
			intf.migration_or_duplication = 1 #Set true for performance.
			if self.tid:
				intf.include_thread_name = self.tid
			
			res = self.my_encp.do_work(intf)
			if res == None:
				#return -10
				res = -10  #Same as initial value.

			self.err_msg = self.my_encp.err_msg[thread.get_ident()]
		except (KeyboardInterrupt, SystemExit):
			Trace.logname = logname #Reset the log file name.
			raise sys.exc_info()[0], sys.exc_info()[1], \
			      sys.exc_info()[2]
		except:
			self.err_msg = str((str(sys.exc_info()[0]),
					    str(sys.exc_info()[1])))
			sys.stderr.write("%s\n" % self.err_msg)
			res = 1

		#If we end up with encp being owned not by root at this
		# point, we need to set it back.
		file_utils.acquire_lock_euid_egid()
		try:
			file_utils.set_euid_egid(0, 0)
		except (KeyboardInterrupt, SystemExit):
			raise sys.exc_info()[0], sys.exc_info()[1], \
			      sys.exc_info()[2]
		except:
			Trace.logname = logname #Reset the log file name.
			self.err_msg = str((str(sys.exc_info()[0]),
					    str(sys.exc_info()[1])))
			sys.stderr.write("%s\n" % self.err_msg)
			res = 1
		file_utils.release_lock_euid_egid()
		
		Trace.logname = logname #Reset the log file name.
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
