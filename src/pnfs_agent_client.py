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
import time
import errno
import socket
import select
import pprint
import rexec

# enstore imports
#import setpath
import callback
import hostaddr
import option
import generic_client
import backup_client
#import udp_client
import Trace
import e_errors
import file_clerk_client
import cPickle
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
            raise OSError, "can't create files"
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

    def get_path(self,pnfs_id,dirname):
        ticket = { 'work'    : 'get_path',
                   'pnfs_id' : pnfs_id,
                   'dirname' : dirname,
                   'path'    : None
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
        return ticket['rc']

    def set_bit_file_id(self,fname,bfid):
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

    def set_file_size(self,fname,size):
        ticket = {'work'  : 'set_file_size',
                  'fname' : fname,
                  'size'  : size
                  }
        ticket=self.send(ticket)
        return

    def set_xreference(self, volume, location_cookie, size, file_family,
                       pnfsFilename, volume_filepath, id, volume_fileP,
                       bit_file_id, drive, crc, filepath=None):
        
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

    def set_outfile_permissions(self,work_ticket):
        ticket = { 'work' : 'set_outfile_permissions',
                   'ticket' : work_ticket
                   }
        ticket=self.send(ticket)
        return ticket['ticket']

class PnfsAgentClientInterface(generic_client.GenericClientInterface):
    def __init__(self, args=sys.argv, user_mode=1):

        generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)
        return

    def valid_dictionaries(self):
        return (self.alive_options, self.help_options, self.trace_options,
                self.volume_options)


if __name__ == "__main__":
    Trace.init(MY_NAME)
    Trace.trace( 6, 'pac called with args: %s'%(sys.argv,) )

    # fill in the interface
    intf = PnfsAgentClientInterface(user_mode=0)

    do_work(intf)

