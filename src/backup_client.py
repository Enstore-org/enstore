###############################################################################
# src/$RCSfile$   $Revision$
#
#enstore imports
import Trace

class BackupClient:

    def start_backup(self):
        Trace.trace(10,'{start_backup')
        r = self.send({'work':'start_backup'})
        Trace.trace(10,'}start_backup '+repr(r))
        return r

    def stop_backup(self):
        Trace.trace(10,'{stop_backup')
        r = self.send({'work':'stop_backup'})
        Trace.trace(10,'}stop_backup '+repr(r))
        return r
