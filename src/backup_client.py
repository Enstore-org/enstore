###############################################################################
# src/$RCSfile$   $Revision$
#
class BackupClient:

    def start_backup(self):
    	return self.send({'work':'start_backup'})

    def stop_backup(self):
    	return self.send({'work':'stop_backup'})	
