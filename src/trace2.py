#############################################################
# $Id$

import sys,types,string

try:
    from Trace import trace
except ImportError:
    def trace(level,message):
        print "    "*level+message

def __format_output(level, tb, prefix_char, vars):
    d = {}
    d.update(tb.f_globals)
    d.update(tb.f_locals)
    function_name = tb.f_code.co_name
    module_name = d['__name__']
    l = []
    for v in vars:
        if d.has_key(v):
            l.append("%s=%s"%(v,d[v]))
        else:
            l.append(str(v))
    trace(level, "%s%s.%s: %s" % (prefix_char, module_name,
                                  function_name, string.join(l)))
    
def __generic(level,prefix_char,vars):
    try:
        raise "NoError"
    except:
        x = sys.exc_info()
    tb = x[2].tb_frame.f_back.f_back
    __format_output(level, tb, prefix_char, vars)

def entering(level, *vars):
    """logs the entering of a function to the trace buffer
    automatically dumps values of function arguments """
    try:
        raise "NoError"
    except:
        x = sys.exc_info()
    tb = x[2].tb_frame.f_back
    code = tb.f_code
    args = code.co_varnames[:code.co_nlocals]
    __format_output(level, tb, "{", args+vars)

    
def leaving(level, *vars):
    """logs the exiting of a function to the trace buffer"""
    __generic(level,"}",vars)

def status(level, *vars):
    """logs a status message to the trace buffer"""
    __generic(level,"",vars)


        
