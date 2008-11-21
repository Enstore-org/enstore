#!/usr/bin/env python

# $Id$

# Wrapper code for builtin eval() to run in restricted mode/environment.

# system imports
import re
import compiler
import sys

# looking for "CallFunc("
re_CallFunc = re.compile("CallFunc\(")

# en_eval(expr) -- safer eval
#
# it rejects (1) non-string input
#            (2) function invocation
# and it does not allow access to any local or global symbols

def en_eval(expr, debug=False):
	# reject anything that is NOT a string
	if type(expr) != type(""):
		if debug:
			sys.stderr.write("en_eval Error: not a string type\n")
		#return None
		raise TypeError("expected string not %s" % (type(expr),))

	# reject function invocation
	if re_CallFunc.search(str(compiler.parse(expr).node.nodes)) != None:
		if debug:
			sys.stderr.write("en_eval Error: function invocation\n")
		#return None
		raise SyntaxError("functions not allowed")

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
		#return None
		raise SyntaxError("empty string not expected")
 

	# no access to globals nor locals
	try:
		val = eval(expr, {}, {})
	except SyntaxError, msg:
		if debug:
			sys.stderr.write("en_eval Error: %s parsing string: %s\n" % (str(msg), expr))
		#return None
		raise SyntaxError, msg

	return val
