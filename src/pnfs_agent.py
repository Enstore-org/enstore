#!/usr/bin/env python

###############################################################################
#
# $Id$
# $Author$
#
# pnfs agent is server sitting on a host that has pnfs mounted
# encp talks to this agent via pnfs_agent_client
#
###############################################################################

# system imports
import sys
import os
import time
import errno
import string
import socket
import select
import stat

# enstore imports
import hostaddr
import callback
import dispatching_worker
import generic_server
import edb
import Trace
import e_errors
import configuration_client
import volume_family
import esgdb
import enstore_constants
import monitored_server
import inquisitor_client
import cPickle
import event_relay_messages
import pnfs

MY_NAME = enstore_constants.PNFS_AGENT   #"pnfs_agent"

default_pinfo = {"inode" : 0,
                #"gid" : None,
                #"gname" : None,
                #"uid" : None,
                #"uname" : None,
                "major" : None,
                "minor" : None,
                "rmajor" : None,
                "rminor" : None,
                "mode" : None,
                "pnfsFilename" : None,
                }

class PnfsAgent(dispatching_worker.DispatchingWorker,
                generic_server.GenericServer) :
    
    def __init__(self, csc, name=MY_NAME):
        generic_server.GenericServer.__init__(self, csc, name,
                                              function = self.handle_er_msg)        
        self.name       = name
        self.shortname  = name
        self.keys       = self.csc.get(self.name)
        dispatching_worker.DispatchingWorker.__init__(self, (self.keys['hostip'], self.keys['port']))
        self.dict       = None
        self.set_error_handler(self.handle_error)
        Trace.init(self.log_name)
	self.alive_interval = monitored_server.get_alive_interval(self.csc,
								  MY_NAME,
								  self.keys)
    def handle_error(self, exc, msg, tb):
        x = tb 
        Trace.log(e_errors.ERROR, "handle pnfs agent error %s %s"%(exc, msg))
        Trace.trace(10, "%s %s" %(self.current_work_ticket, state_name(self.state)))
        if self.current_work_ticket:
            try:
                Trace.trace(10, "handle error: calling transfer failed, str(msg)=%s"%(str(msg),))
                self.transfer_failed(exc, msg)
            except:
                pass

    # show_state -- show internal configuration values
    def show_state(self, ticket):
        ticket['state'] = {}
        for i in self.__dict__.keys():
            ticket['state'][i] = `self.__dict__[i]`
        ticket['status'] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    def get_file_stat(self, ticket):
        filename = ticket['filename']
        if ( os.path.exists(filename) ) :
            if ( pnfs.is_pnfs_path(filename) ):
                tmp = os.stat(filename)
                filesize = tmp[stat.ST_SIZE]
                pin = pnfs.Pnfs(filename)
                bfid = pin.get_bit_file_id()
                ticket['bfid']=bfid
                pinfo = {}
                for k in [ 'pnfsFilename', 'gid', 'gname', 'uid', 'uname',
                           'major', 'minor', 'rmajor', 'rminor',
                           'mode', 'pstat', 'inode' ]:
                    try:
                        pinfo[k] = getattr(pin, k)
                    except AttributeError:
                        if default_pinfo.has_key(k):
                            pinfo[k] = default_pinfo[k]
                ticket['pinfo'] = pinfo
                if ( filesize == 1 ) :
                    pin.pstatinfo()
                    pin.get_file_size()
                    filesize = pin.file_size
                for i in range(0,len(tmp)):
                    ticket['statinfo'].append(tmp[i])
                ticket['statinfo'][stat.ST_SIZE]=filesize
                ticket['status']   = (e_errors.OK, None)
            else:
                msg="file %s exists but not PNFS file"%filename
                Trace.log(e_errors.ERROR, msg)
                ticket['status'] = (e_errors.DOESNOTEXIST, None)
        else:
            msg="file %s does not exist on PNFS"%filename
            Trace.log(e_errors.ERROR, msg)
            ticket['status'] = (e_errors.DOESNOTEXIST, None)
        self.reply_to_caller(ticket)
        return

    def get_library(self, ticket):
        dirname = ticket['dirname']
        if ( os.path.exists(dirname) ) :
            if ( os.path.isdir(dirname) ) :
                t = pnfs.Tag(dirname)
                ticket['library']=t.get_library().split(",")[0]
                ticket['status']   = (e_errors.OK, None)
            else:
                msg="%s not a directory"%dirname
                Trace.log(e_errors.ERROR, msg)
                ticket['status'] = (e_errors.DOESNOTEXIST, None)
              
        else:
            msg="directory %s does not exist"%dirname
            Trace.log(e_errors.ERROR, msg)
            ticket['status'] = (e_errors.DOESNOTEXIST, None)
            
        self.reply_to_caller(ticket)
        return

    def get_file_family(self, ticket):
        dirname = ticket['dirname']
        if ( os.path.exists(dirname) ) :
            if ( os.path.isdir(dirname) ) :
                t = pnfs.Tag(dirname)
                ticket['file_family']=t.get_file_family()
                ticket['status']   = (e_errors.OK, None)
            else:
                msg="%s not a directory"%dirname
                Trace.log(e_errors.ERROR, msg)
                ticket['status'] = (e_errors.DOESNOTEXIST, None)
              
        else:
            msg="directory %s does not exist"%dirname
            Trace.log(e_errors.ERROR, msg)
            ticket['status'] = (e_errors.DOESNOTEXIST, None)
        self.reply_to_caller(ticket)
        return

    def get_file_family_width(self, ticket):
        dirname = ticket['dirname']
        if ( os.path.exists(dirname) ) :
            if ( os.path.isdir(dirname) ) :
                t = pnfs.Tag(dirname)
                ticket['file_family_width']=t.get_file_family_width()
                ticket['status']   = (e_errors.OK, None)
            else:
                msg="%s not a directory"%dirname
                Trace.log(e_errors.ERROR, msg)
                ticket['status'] = (e_errors.DOESNOTEXIST, None)
              
        else:
            msg="directory %s does not exist"%dirname
            Trace.log(e_errors.ERROR, msg)
            ticket['status'] = (e_errors.DOESNOTEXIST, None)
        self.reply_to_caller(ticket)
        return


class PnfsAgentInterface(generic_server.GenericServerInterface):
        pass

if __name__ == "__main__":
    Trace.init(string.upper(MY_NAME))

    # get the interface
    intf = PnfsAgentInterface()
    vc   = PnfsAgent((intf.config_host, intf.config_port))
    vc.handle_generic_commands(intf)
    
    Trace.log(e_errors.INFO, '%s' % (sys.argv,))


    while 1:
        try:
            vc.serve_forever()
        except SystemExit:
            Trace.log(e_errors.INFO, "%s exiting." % (vc.name,))
            os._exit(0)
            break
        except:
            try:
                exc, msg, tb = sys.exc_info()
                full_tb = traceback.format_exception(exc,msg,tb)
                for l in full_tb:
                    Trace.log(e_errors.ERROR, l[:-1], {}, "TRACEBACK")
                Trace.log(e_errors.INFO, "restarting after exception")
                vc.restart()
            except:
                pass

    Trace.log(e_errors.INFO, 'ERROR returned from serve_forever')
