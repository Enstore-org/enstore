###############################################################################
# src/$RCSfile$   $Revision$
#
"""
Defines function selecting a wrapper instance
"""
import imp

def select_wrapper(wrapper_module):
    try:
	(file, pathname, description)=imp.find_module(wrapper_module)
	module = imp.load_module(wrapper_module, file, pathname, description)
	wrapper = module.Wrapper()
	return (wrapper)
    except:
	return None
