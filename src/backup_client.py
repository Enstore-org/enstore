###############################################################################
# src/$RCSfile$   $Revision$
#
#enstore imports
import Trace

class BackupClient:

    def start_backup(self):
        r = self.send({'work':'start_backup'})
        return r

    def stop_backup(self):
        r = self.send({'work':'stop_backup'})
        return r
