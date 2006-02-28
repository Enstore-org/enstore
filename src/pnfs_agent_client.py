#!/usr/bin/env python
###############################################################################
#
# $Author$
# $Date$
# $Id$
#
# PNFS agend client 
# Author: Dmitry Litvintsev (litvinse@fnal.gov) 08/05
#
###############################################################################

# system imports
import sys
import string
# import time
import errno
import socket
# import select
import pprint
# import rexec
import stat

# enstore imports
#import setpath
import hostaddr
import option
import generic_client
import backup_client
#import udp_client
import Trace
import e_errors
# import cPickle
import info_client
import enstore_constants

MY_NAME = enstore_constants.PNFS_AGENT_CLIENT  #"PNFS_A_CLIENT"
MY_SERVER = enstore_constants.PNFS_AGENT     #"pnfs_agent"

RCV_TIMEOUT = 10
RCV_TRIES = 5

class PnfsAgentClient(generic_client.GenericClient,
                      backup_client.BackupClient):

    def __init__( self, csc, server_address=None, flags=0, logc=None,
                  alarmc=None, rcv_timeout = RCV_TIMEOUT,
                  rcv_tries = RCV_TRIES):

        generic_client.GenericClient.__init__(self,csc,MY_NAME,server_address,
                                              flags=flags, logc=logc,
                                              alarmc=alarmc,
                                              rcv_timeout=rcv_timeout,
                                              rcv_tries=rcv_tries,
                                              server_name = MY_SERVER)

        self.r_ticket = {'work'          : 'get_file_stat',
                         'filename'      : "",
                         'statinfo'      : [],
                         'pinfo'         : {},
                         'bfid'          : None
                       }
    def status(self, rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        return self.send({"work" : "show_state"}, rcv_timeout, tries)

    def file_size(self):
        return self.r_ticket['statinfo'][stat.ST_SIZE]
        
    def show_state(self):
        return self.send({'work':'show_state'})

    def get_file_stat(self,filename) :
        if ( self.r_ticket['filename'] != filename ) :
            self.r_ticket['filename'] = filename 
            self.r_ticket=self.send(self.r_ticket)
            if self.r_ticket['status'][0] == e_errors.OK:
                return self.r_ticket['statinfo'], self.r_ticket['bfid'], self.r_ticket['pinfo']
            else:
                return None, None, None
        else:
            if ( self.r_ticket['status'][0] == e_errors.OK ) :
                return self.r_ticket['statinfo'], self.r_ticket['bfid'], self.r_ticket['pinfo']
            else:
                return None, None, None

    def get_pnfsstat(self, filename):
        ticket = {'work'          : 'get_pnfsstat',
                  'filename'      : filename,
                  }
        ticket=self.send(ticket)
        if e_errors.is_ok(ticket):
            return ticket['statinfo']
        elif ticket['status'][0] == e_errors.IOERROR:
            raise IOError, (ticket['errno'], ticket['status'][1])
        else:
            raise OSError, (ticket.get('errno', e_errors.UNKNOWN),
                            ticket['status'][1])

    def get_stat(self, filename):
        ticket = {'work'          : 'get_stat',
                  'filename'      : filename,
                  }
        ticket=self.send(ticket)
        if e_errors.is_ok(ticket):
            return ticket['statinfo']
        elif ticket['status'][0] == e_errors.IOERROR:
            raise IOError, (ticket['errno'], ticket['status'][1])
        else:
            raise OSError, (ticket.get('errno', e_errors.UNKNOWN),
                            ticket['status'][1])

    def get_pinfo(self,filename) :
        ticket = {'work'          : 'get_pinfo',
                  'filename'      : filename,
                  'pinfo'         : {}
                  }
        ticket=self.send(ticket)
        if ticket['status'][0] == e_errors.OK:
            return ticket['pinfo']
        else:
            return None

    def get_library(self,dirname):
        ticket = {'work'          : 'get_library',
                  'dirname'       : dirname,
                  'library'       : None
                  }
        ticket=self.send(ticket)
        if ( ticket['status'][0] == e_errors.OK ):
            return ticket['library']
        else:
            return None
        
    def get_file_family(self,dirname):
        ticket = {'work'          : 'get_file_family',
                  'dirname'       : dirname,
                  'file_family'   : None
                  }
        ticket=self.send(ticket)
        if ( ticket['status'][0] == e_errors.OK ):
            return ticket['file_family']
        else:
           return None

    def get_file_family_width(self,dirname):
        ticket = {'work'                : 'get_file_family_width',
                  'dirname'             : dirname,
                  'file_family_width'   : None
                  }
        ticket=self.send(ticket)
        if ( ticket['status'][0] == e_errors.OK ):
            return ticket['file_family_width']
        else:
           return None

    def get_file_family_wrapper(self,dirname):
        ticket = {'work'                : 'get_file_family_wrapper',
                  'dirname'             : dirname,
                  'file_family_wrapper'   : None
                  }
        ticket=self.send(ticket)
        if ( ticket['status'][0] == e_errors.OK ):
            return ticket['file_family_wrapper']
        else:
           return None

    def get_storage_group(self,dirname):
        ticket = {'work'                : 'get_storage_group',
                  'dirname'             : dirname,
                  'storage_group'      : None
                  }
        ticket=self.send(ticket)
        if ( ticket['status'][0] == e_errors.OK ):
            return ticket['storage_group']
        else:
           return None

    def create_zero_length_pnfs_files(self,filenames):
        ticket = { 'work'      : 'create_zero_length_pnfs_files',
                   'filenames' : filenames,
                   'msg'       : None
                   }
        ticket=self.send(ticket)
        if ( ticket['status'][0] != e_errors.OK ):
            raise OSError, ticket['status'][1]
        else:
            filenames = ticket['filenames']
            return filenames[0]

    def get_ninfo(self,inputfile,outputfile,inlen) :
        ticket = { 'work'       : 'get_ninfo',
                   'inputfile'  : inputfile,
                   'outputfile' : outputfile,
                   'inlen'      : inlen
                   }
        ticket=self.send(ticket)
        return ticket['inputfile'],ticket['outputfile']

    def get_path(self,pnfs_id,mount_point,shortcut=None):
        ticket = { 'work'    : 'get_path',
                   'pnfs_id' : pnfs_id,
                   'dirname' : mount_point,
                   'shortcut' : shortcut,
                   'path' : None
                   }
        ticket=self.send(ticket)
        return ticket['path']

    def e_access(self,path,mode):
        ticket = { 'work' : 'e_access',
                   'path' : path,
                   'mode' : mode,
                   'rc'   : 1
                   }
        ticket=self.send(ticket)
        if not e_errors.is_ok(ticket):
            return False
        return ticket['rc']

    def set_bit_file_id(self,bfid,fname):
        ticket = {'work'  : 'set_bit_file_id',
                  'fname' : fname,
                  'bfid'  : bfid
                  }
        ticket=self.send(ticket)
        return 

    def get_bit_file_id(self,fname):
        ticket = {'work'  : 'get_bit_file_id',
                  'fname' : fname,
                  'bfid'  : None
                  }
        ticket=self.send(ticket)
        return ticket['bfid']

    def get_id(self,fname):
        ticket = {'work'  : 'get_id',
                  'fname' : fname,
                  'file_id'  : None
                  }
        ticket=self.send(ticket)
        return ticket['file_id']

    def get_parent_id(self,pnfsid):
        ticket = {'work'  : 'get_parent_id',
                  'pnfsid' : pnfsid,
                  'parent_id'  : None
                  }
        ticket=self.send(ticket)
        return ticket['parent_id']

    def get_file_size(self, fname):
        ticket = {'work'  : 'get_file_size',
                  'fname' : fname,
                  }
        ticket=self.send(ticket)
        if not e_errors.is_ok(ticket):
            return None  #This is what pnfs.get_file_size() does on error.
        return ticket['size']

    def set_file_size(self,size,fname):
        ticket = {'work'  : 'set_file_size',
                  'fname' : fname,
                  'size'  : size
                  }
        ticket=self.send(ticket)
        return

    def set_xreference(self, volume, location_cookie, size, file_family,
                       pnfsFilename, volume_filepath, id, volume_fileP,
                       bit_file_id, drive, crc, filepath):
        
        ticket = {'work'             : 'set_xreference',
                  'volume'           : volume,
                  'location_cookie'  : location_cookie,
                  'size'             : size,
                  'file_family'      : file_family,
                  'pnfsFilename'     : pnfsFilename,
                  'volume_filepath'  : volume_filepath,
                  'id'               : id,
                  'volume_fileP'     : volume_fileP,
                  'bit_file_id'      : bit_file_id,
                  'drive'            : drive,
                  'crc'              : crc,
                  'filepath'         : filepath
                  }
        ticket=self.send(ticket)
        return

    def get_xreference(self, fname):
        ticket = {'work' : 'get_xreference',
                  'fname' : fname,
                  }
        ticket = self.send(ticket)
        if e_errors.is_ok(ticket):
            return ticket['xref']
        raise OSError, ticket['status'][1]

    def readlayer(self, fname, layer):
        ticket = {'work' : 'readlayer',
                  'fname' : fname,
                  'layer' : layer
                  }
        ticket = self.send(ticket)
        if e_errors.is_ok(ticket):
            return ticket['contents']
        return [] #What sould happen here?

    def writelayer(self, layer, value, fname):
        ticket = {'work' : 'writelayer',
                  'fname' : fname,
                  'layer' : layer,
                  'value' : value
                  }
        ticket = self.send(ticket)
        if not e_errors.is_ok(ticket):
            raise OSError, ticket['status'][1]

    def set_outfile_permissions(self,work_ticket):
        ticket = { 'work' : 'set_outfile_permissions',
                   'ticket' : work_ticket
                   }
        ticket=self.send(ticket)
        return ticket['ticket']

    def chmod(self,mode,filename):
        ticket = { 'work' : 'chmod',
                   'fname' : filename,
                   'mode' : mode
                   }
        ticket=self.send(ticket)
        if e_errors.is_ok(ticket):
            return
        elif ticket['status'][0] == e_errors.IOERROR:
            raise IOError, (ticket['errno'], ticket['status'][1])
        else:
            raise OSError, (ticket.get('errno', e_errors.UNKNOWN),
                            ticket['status'][1])

    def is_pnfs_path(self, filename, check_name_only = None):
        ticket = { 'work' : 'is_pnfs_path',
                   'fname' : filename,
                   'check_name_only' : check_name_only
                   }
        ticket = self.send(ticket)
        if not e_errors.is_ok(ticket):
            return False   #Should this raise an exception instead?
        return ticket['rc']

    def get_directory_name(self, filename):
        ticket = { 'work' : 'is_pnfs_path',
                   'fname' : filename,
                   }
        ticket = self.send(ticket)
        if not e_errors.is_ok(ticket):
            return None   #Should this raise an exception instead?
        return ticket['rc']

    def isdir(self, filename):
        ticket = {'work'          : 'get_stat',
                  'filename'      : filename,
                  }
        ticket=self.send(ticket)
        if not e_errors.is_ok(ticket):
            return None
        else:
            return stat.S_ISDIR(ticket['statinfo'][stat.ST_MODE])

    def isfile(self, filename):
        ticket = {'work'          : 'get_stat',
                  'filename'      : filename,
                  }
        ticket=self.send(ticket)
        if not e_errors.is_ok(ticket):
            return None
        else:
            return stat.S_ISREG(ticket['statinfo'][stat.ST_MODE])

    def islink(self, filename):
        ticket = {'work'          : 'get_stat',
                  'filename'      : filename,
                  }
        ticket=self.send(ticket)
        if not e_errors.is_ok(ticket):
            return None
        else:
            return stat.S_ISLNK(ticket['statinfo'][stat.ST_MODE])

    # create a new file or update its times
    def touch(self, filename):
        ticket = {'work'          : 'touch',
                  'filename'      : filename,
                  }
        ticket=self.send(ticket)
        if ticket['status'][0] == e_errors.OK:
            return
        elif ticket['status'][0] == e_errors.IOERROR:
            raise IOError, (ticket['errno'], ticket['status'][1])
        else:
            raise OSError, (ticket['errno'], ticket['status'][1])

    # create a new file
    def creat(self, filename, mode = None):
        ticket = {'work'          : 'creat',
                  'filename'      : filename,
                  }
        if mode:
            ticket['mode'] = mode
        ticket=self.send(ticket)
        if ticket['status'][0] == e_errors.OK:
            return
        elif ticket['status'][0] == e_errors.IOERROR:
            raise IOError, (ticket['errno'], ticket['status'][1])
        else:
            raise OSError, (ticket['errno'], ticket['status'][1])

    # update the access and mod time of a file
    def utime(self, filename):
        ticket = {'work'          : 'utime',
                  'fname'         : filename,
                  }
        ticket=self.send(ticket)
        if ticket['status'][0] == e_errors.OK:
            return
        elif ticket['status'][0] == e_errors.IOERROR:
            raise IOError, (ticket['errno'], ticket['status'][1])
        else:
            raise OSError, (ticket['errno'], ticket['status'][1])
        
    # delete a pnfs file including its metadata
    def rm(self, filename):
        ticket = {'work'          : 'rm',
                  'fname'         : filename,
                  }
        ticket=self.send(ticket)
        if ticket['status'][0] == e_errors.OK:
            return
        elif ticket['status'][0] == e_errors.IOERROR:
            raise IOError, (ticket['errno'], ticket['status'][1])
        else:
            raise OSError, (ticket['errno'], ticket['status'][1])
        
    # delete a pnfs file (leaving the metadata do be put in the trashcan)
    def remove(self, filename):
        ticket = {'work'          : 'remove',
                  'fname'         : filename,
                  }
        ticket=self.send(ticket)
        if ticket['status'][0] == e_errors.OK:
            return
        elif ticket['status'][0] == e_errors.IOERROR:
            raise IOError, (ticket['errno'], ticket['status'][1])
        else:
            raise OSError, (ticket['errno'], ticket['status'][1])
        

class PnfsAgentClientInterface(generic_client.GenericClientInterface):
    def __init__(self, args=sys.argv, user_mode=1):
        self.alive_rcv_timeout = RCV_TIMEOUT
        self.alive_retries = RCV_TRIES
        self.enable = 0
        self.status = 0
        self.notify = []
        self.sendto = []
        self.dump = 0
        self.warm_restart = 0
        generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)
        return

    def valid_dictionaries(self):
        return (self.alive_options, self.help_options, self.trace_options,
                self.pnfs_agent_options)

    pnfs_agent_options = {
        option.STATUS:{option.HELP_STRING:"print pnfs_agent status",
                       option.DEFAULT_VALUE:option.DEFAULT,
                       option.DEFAULT_TYPE:option.INTEGER,
                       option.VALUE_USAGE:option.IGNORED,
                       option.USER_LEVEL:option.ADMIN},
        }

def do_work(intf):
    pac = PnfsAgentClient((intf.config_host, intf.config_port),
                          rcv_timeout = intf.alive_rcv_timeout,
                          rcv_tries = intf.alive_retries)
    Trace.init(pac.get_name(MY_NAME))
    ticket = {}
    try:
        ticket = pac.handle_generic_commands(MY_SERVER, intf)
        if ticket:
            pass
        elif intf.status:
            ticket = pac.status(intf.alive_rcv_timeout,intf.alive_retries)
            pprint.pprint(ticket)
        else:
            intf.print_help()
            sys.exit(0)
    except (KeyboardInterrupt,SystemExit):
        sys.exit(1)
         



if __name__ == "__main__":
    Trace.init(MY_NAME)
    Trace.trace( 6, 'pac called with args: %s'%(sys.argv,) )

    # fill in the interface
    intf = PnfsAgentClientInterface(user_mode=0)

    do_work(intf)

