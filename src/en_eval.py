#!/usr/bin/env python

# $Id$

# Wrapper code for builtin eval() to run in restricted mode/environment.

# system imports
import re
import compiler
import sys
import time
import Trace

# looking for "CallFunc("
re_CallFunc = re.compile("CallFunc\(")

# en_eval(expr) -- safer eval
#
# it rejects (1) non-string input
#            (2) function invocation
# and it does not allow access to any local or global symbols

def en_eval(expr, debug=False):
	Trace.trace(5,"en_eval %s"%(expr,))
	t0=time.time()
	# reject anything that is NOT a string
	if type(expr) != type(""):
		if debug:
			sys.stderr.write("en_eval Error: not a string type\n")
		#return None
		raise TypeError("expected string not %s" % (type(expr),))

	# reject function invocation
	t01=time.time()
	fun = str(compiler.parse(expr).node.nodes)
	t02=time.time()
	#if re_CallFunc.search(str(compiler.parse(expr).node.nodes)) != None:
	if re_CallFunc.search(fun) != None:
		if debug:
			sys.stderr.write("en_eval Error: function invocation\n")
		#return None
		raise SyntaxError("functions not allowed")
	
	t03=time.time()
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
		t=time.time()
		val = eval(expr, {}, {})
		t1=time.time()
		Trace.trace(5,"en_eval %s %s %s %s"%(t01-t0, t02-t01,t03-t02,t1-t,))
	except SyntaxError, msg:
		if debug:
			sys.stderr.write("en_eval Error: %s parsing string: %s\n" % (str(msg), expr))
		#return None
		raise SyntaxError, msg
	except NameError, detail:
		Trace.trace(5, "NameError %s %s"%(detail, expr))
		raise NameError, expr

	return val
