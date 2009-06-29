#!/usr/bin/env python
import sys
import os
import enstore_functions2
import configuration_client
import pnfs
import enstore_constants
import pnfs_agent_client

def usage(cmd):
    print "Usage: %s <pnfs_path> <list_file>"%(cmd,)

# creates a list of bit file ids to be used in read_test.sh
class BFIDList:
    def __init__(self, starting_directory, output_file):
        config_host = enstore_functions2.default_host()
        config_port = enstore_functions2.default_port()
        self.csc = configuration_client.ConfigurationClient((config_host,
                                                             config_port))
        self.p = None
        self.base_dir = starting_directory
        self.use_pnfs_agent=os.getenv('REMOTE_ENCP')
        if self.use_pnfs_agent:         
            info = self.csc.get('pnfs_agent', {})
            if info:
                flags = enstore_constants.NO_LOG | enstore_constants.NO_ALARM
                pac_addr = (info.get('hostip', None),
                            info.get('port', None))
                
                self.p = pnfs_agent_client.PnfsAgentClient(self.csc, flags = flags,
                                                             logc = None,
                                                             alarmc = None,
                                                             server_address = pac_addr)
        else:
            self.p = pnfs.Pnfs()
        self.list_file = open(output_file, 'w')


    def create_list(self, dir):
        for root, dirs, files in os.walk(sys.argv[1]):
            print "ROOT %s DIRS %s"%(root, dirs)
            for f in files:
                print "FILE", f
                bfid = self.p.get_bit_file_id(os.path.join(root,f))
                if bfid:
                    self.list_file.write("%s\n"%(bfid))

    
if __name__ == '__main__':
    if len(sys.argv) < 3:
        usage(sys.argv[0])
    else:
        lst = BFIDList(sys.argv[1], sys.argv[2])
        lst.create_list(sys.argv[1])
        lst.list_file.close()
