###############################################################################
# src/$RCSfile$   $Revision$
#
import time

def tod() :
    return time.strftime("%c",time.localtime(time.time()))
