###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import time

#enstore imports

def tod() :
    return time.strftime("%c",time.localtime(time.time()))
