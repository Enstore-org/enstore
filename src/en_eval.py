#!/usr/bin/env python

import re
import compiler

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
			print "Error: not a string type"
		return None

	# reject function invocation
	if re_CallFunc.search(str(compiler.parse(expr).node.nodes)) != None:
		if debug:
			print "Error: function invocation"
		return None

	# no access to globals nor locals
	return eval(expr, {}, {})
