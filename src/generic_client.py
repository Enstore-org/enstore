###############################################################################
# src/$RCSfile$   $Revision$
#
#enstore imports
import Trace
class GenericClient:

    # check on alive status
    def alive(self):
        Trace.trace(10,'{alive')
        x = self.send({'work':'alive'})
        Trace.trace(10,'}alive '+repr(x))
        return x
