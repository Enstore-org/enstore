###############################################################################
# src/$RCSfile$   $Revision$
#
"""
Defines function selecting a wrapper instance
"""


def select_wrapper(wrapper_module):
    try:
	exec("import "+wrapper_module)
	return eval(wrapper_module+".Wrapper()")
    except:
	
	return None
