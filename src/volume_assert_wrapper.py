#!/usr/bin/env python
"""
a quick and not so dirty wrapper for volume_assert so that multiple
volume_asserts can be instantiated in the same program, probably in
different threads.
"""

import string
import sys

import Trace

class VolumeAssert:
	def __init__(self, tid=None):
		import volume_assert
		self.my_volume_assert = volume_assert
		self.tid = tid
		self.exit_status = -10
		self.err_msg = ""

	# volume_assert(cmd) -- cmd is the same as the command line
	# eg. volume_assert("volume_assert --verbose 4 --volume TEST19")
	def volume_assert(self, cmd):
		self.exit_status = -10 #Reset this every time.
		self.err_msgs = ""

                #Insert the command if it is not already there.
                CMD = "volume_assert"
		if cmd[:len(CMD)] != CMD:
			cmd = CMD + " " + cmd
                
		argv = string.split(cmd)
		intf = self.my_volume_assert.VolumeAssertInterface(argv, 0)
		intf.migration_or_duplication = 1 #Set true for performance.
		if self.tid:
			intf.include_thread_name = self.tid
		logname=Trace.logname #Grab this to reset this after volume_assert.
		try:
			res = self.my_volume_assert.do_work(intf)
			if res == None:
				#return -10
				res = -10  #Same as initial value.

			self.err_msgs = self.my_volume_assert.err_msgs
		except (KeyboardInterrupt, SystemExit):
			Trace.logname = logname #Reset the log file name.
			raise sys.exc_info()[0], sys.exc_info()[1], \
			      sys.exc_info()[2]
		except:
			self.err_msgs = [{'status':(str(sys.exc_info()[0]),
                                                    str(sys.exc_info()[1]))}]
			res = 1

		Trace.logname = logname #Reset the log file name.
		self.exit_status = res #Return value if used in a thread.
		return res  #Return value if used directly.

	

if __name__ == '__main__':
	test_volume_assert = VolumeAssert()
	for i in sys.argv[1:]:
		print "copying", i, "...",
		cmd = "volume_assert %s"%(i)
		res = test_volume_assert.volume_assert(cmd)
		if res:
			print "FAILED"
		else:
			print "DONE"
