#############################################################
# $Id$

import sys,types,string

try:
    from Trace import trace
except ImportError:
    def trace(level,message):
        print "    "*level+message


def __format_output(level, tb, prefix_char, exprs):
    function_name = tb.f_code.co_name
    module_name = tb.f_globals['__name__']
    l = []
    for expr in map(str,exprs):
        if expr[-1] == "=":
            try:
                value = str(eval(expr[:-1], tb.f_globals, tb.f_locals))
            except:
                value = "***eval error***"
            expr = expr+value
        l.append(expr)
    trace(level, "%s%s.%s: %s" % (prefix_char, module_name,
                                  function_name, string.join(l)))
    
def __generic(level,prefix_char,exprs):
    try:
        raise "NoError"
    except:
        x = sys.exc_info()
    tb = x[2].tb_frame.f_back.f_back
    __format_output(level, tb, prefix_char, exprs)

def entering(level, *exprs):
    """logs the entering of a function to the trace buffer
    automatically dumps values of function arguments """
    try:
        raise "NoError"
    except:
        x = sys.exc_info()
    tb = x[2].tb_frame.f_back
    code = tb.f_code
    args = []
    for arg in code.co_varnames[:code.co_nlocals]:
        args.append(arg+"=")

    __format_output(level, tb, "{", args+list(exprs))

    
def leaving(level, *exprs):
    """logs the exiting of a function to the trace buffer"""
    __generic(level,"}",exprs)

def status(level, *exprs):
    """logs a status message to the trace buffer"""
    __generic(level,"",exprs)


        
