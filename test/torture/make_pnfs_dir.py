#!/usr/bin/env python

import os
import sys
import getopt
import time
import configuration_client
import pnfs_agent_client
import pnfs
import enstore_functions2
import enstore_constants
import e_errors

# creates a pnfs directiry with default settings.
class MakePnfsDir:
    def __init__(self, sg, library, ff, ff_w, wrapper):
        self.sg = sg
        self.library = library
        self.ff = ff
        self.ff_w = ff_w
        self.wrapper = wrapper
        config_host = enstore_functions2.default_host()
        config_port = enstore_functions2.default_port()
        self.csc = configuration_client.ConfigurationClient((config_host,
                                                             config_port))
        self.pac = None
        self.use_pnfs_agent=os.getenv('REMOTE_ENCP')

        if self.use_pnfs_agent:         
            info = self.csc.get('pnfs_agent', {})
            if info:
                flags = enstore_constants.NO_LOG | enstore_constants.NO_ALARM
                pac_addr = (info.get('hostip', None),
                            info.get('port', None))
                
                self.pac = pnfs_agent_client.PnfsAgentClient(self.csc, flags = flags,
                                                             logc = None,
                                                             alarmc = None,
                                                             server_address = pac_addr)
         

    def mkdir(self, dirname):
        
        if self.use_pnfs_agent and self.use_pnfs_agent=="only_pnfs_agent":
            if self.pac.isdir(dirname):
                
                return 0
            else:
                # try to create directory
                if self.pac.p_mkdirs(dirname, uid=os.getuid(), gid=os.getgid()):
                    ret = 0
                    p = self.pac
                else:
                    return 1

        else:
            if os.path.isdir(dirname):
                return 0
            else:
                p = pnfs.Pnfs()
                try:
                    os.makedirs(dirname)
                    ret = 0
                except OSError, detail:
                    print "OSError", detail
                    ret = 1
                except IOError, detail:
                    print "IOError", detail
                    ret = 1
        if ret == 0:
            if self.use_pnfs_agent and self.use_pnfs_agent=="only_pnfs_agent":
                p = self.pac
            else:
                p = pnfs.Tag(dirname)

            p.set_storage_group(self.sg, dirname)
            p.set_library(self.library, dirname)
            p.set_file_family(self.ff, dirname)
            p.set_file_family_width(self.ff_w, dirname)
            p.set_file_family_wrapper(self.wrapper, dirname)

        return ret

if __name__ == "__main__":
    d = MakePnfsDir(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6])
    d.mkdir(sys.argv[1])
        
        
        
        
            
        
