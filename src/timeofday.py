###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import time

#enstore imports
import Trace

def tod() :
    Trace.trace(20,'{}tod')
    return time.strftime("%c",time.localtime(time.time()))
