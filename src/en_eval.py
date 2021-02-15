#!/usr/bin/env python

# $Id$

# Wrapper code for builtin eval() to run in restricted mode/environment.

# system imports
import re
import compiler
import sys
import types
import time
import Trace

# looking for "CallFunc("
# to be used with compiler
re_CallFunc = re.compile(r"CallFunc\(")

# any alphanum character, or _
# followed by 0 or more of "_",
# followed by 0 or more whitespaces,
# followed by "("
# to be used without complier
# this takes abut 20 times less time for executon
re_func = re.compile(r"[A-Za-z0-9_] *\(")

# en_eval(expr) -- safer eval
#
# it rejects (1) non-string input
#            (2) function invocation
# and it does not allow access to any local or global symbols


def en_eval(expr, debug=False, check=True, compile=False):
    # check - check message
    # if checked once there is no need to check again
    # compile - use complier - compiler is deprecated anyway

    Trace.trace(5, "en_eval %s" % (expr,))
    t0 = time.time()
    # reject anything that is NOT a string
    if not isinstance(expr, bytes):
        if debug:
            sys.stderr.write("en_eval Error: not a string type\n")
        # return None
        raise TypeError("expected string not %s" % (type(expr),))

    # reject function invocation
    if check:
        t02 = t01 = time.time()
        if compile:
            fun = str(compiler.parse(expr).node.nodes)
            t02 = time.time()
            rc = re_CallFunc.search(fun)
        else:
            rc = re_func.search(expr)
        if rc is not None:

            if debug:
                sys.stderr.write("en_eval Error: function invocation\n")
                # return None
                raise SyntaxError("functions not allowed")

        t03 = time.time()
        Trace.trace(5, "en_eval %s %s %s" % (t01 - t0, t02 - t01, t03 - t02))

    # reject empty UDP datagrams.
    #
    # On Wednesday, January 30th 2008, it was discovered that
    # "intrusion detection" software called samhain would send an
    # empty UDP datagram to open UDP sockets.  The revalation that
    # that any hacker could initiate a denial of service attack
    # on Enstore inspired this additional error handling.  Otherwise,
    # a traceback occurs disrupting operations and unnecessarily
    # floods the log server with traceback messages.  MZ
    if expr == "":
        if debug:
            sys.stderr.write("en_eval Error: empty UDP datagram ignored\n")
        # return None
        raise SyntaxError("empty string not expected")

    # no access to globals nor locals
    try:
        t = time.time()
        val = eval(expr, {}, {})
        t1 = time.time()
        Trace.trace(5, "en_eval:eval %s" % (t1 - t,))
    except SyntaxError as msg:
        if debug:
            sys.stderr.write(
                "en_eval Error: %s parsing string: %s\n" %
                (str(msg), expr))
        # return None
        raise sys.exc_info()
    except NameError as detail:
        Trace.trace(5, "NameError %s %s" % (detail, expr))
        val = expr
        #raise NameError, expr

    return val
