###############################################################################
# src/$RCSfile$   $Revision$
#
#enstore imports
import Trace
class GenericClient:

    # check on alive status
    def alive(self, rcv_timeout=0, tries=0):
        Trace.trace(10,'{alive')
        x = self.send({'work':'alive'},rcv_timeout,tries)
        Trace.trace(10,'}alive '+repr(x))
        return x
