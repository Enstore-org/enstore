#!/usr/bin/env python

# $Id$

# Wrapper code for builtin eval() to run in restricted mode/environment.

# system imports
import sys
import time
import ast

import Trace

# en_eval(expr) -- safer eval
#
# it rejects (1) non-string input
#            (2) function invocation
# and it does not allow access to any local or global symbols

def en_eval(expr, debug=False):

	Trace.trace(5,"en_eval %s"%(expr,))
	t0=time.time()

	# reject empty UDP datagrams.
	#
	## On Wednesday, January 30th 2008, it was discovered that
	## "intrusion detection" software called samhain would send an
	## empty UDP datagram to open UDP sockets.  The revalation that
	## that any hacker could initiate a denial of service attack
	## on Enstore inspired this additional error handling.  Otherwise,
	## a traceback occurs disrupting operations and unnecessarily
	## floods the log server with traceback messages.  MZ
	if expr == "":
		if debug:
			sys.stderr.write("en_eval Error: empty UDP datagram ignored\n")
		raise SyntaxError("empty string not expected")


	try:
		t=time.time()
		val = ast.literal_eval(expr)
		t1=time.time()
		Trace.trace(5,"en_eval:eval %s"%(t1-t,))
	except SyntaxError, msg:
		if debug:
			sys.stderr.write("en_eval Error: %s parsing string: %s\n" % (str(msg), expr))
		raise sys.exc_info()
	except NameError, detail:
		Trace.trace(5, "NameError %s %s"%(detail, expr))
		val = expr
	return val
