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
        
    def show_state(self):
        return self.send({'work':'show_state'})

    def get_file_stat(self,filename) :
        if ( self.r_ticket['filename'] != filename ) :
            self.r_ticket['filename'] = filename 
            self.r_ticket=self.send(self.r_ticket,5,1)
            if self.r_ticket['status'][0] == e_errors.OK:
                return self.r_ticket['statinfo'], self.r_ticket['bfid'], self.r_ticket['pinfo']
            else:
                return None, None, None
        else:
            if ( self.r_ticket['status'][0] == e_errors.OK ) :
                return self.r_ticket['statinfo'], self.r_ticket['bfid'], self.r_ticket['pinfo']
            else:
                return None, None, None
        
if __name__ == "__main__":
    Trace.init(MY_NAME)
    Trace.trace( 6, 'pac called with args: %s'%(sys.argv,) )

    # fill in the interface
    intf = PnfsAgentClientInterface(user_mode=0)

    do_work(intf)

