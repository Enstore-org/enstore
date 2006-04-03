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
import stat
import types
import traceback

# enstore imports
import hostaddr
import dispatching_worker
import generic_server
import Trace
import e_errors
import enstore_constants
import monitored_server
import event_relay_messages
import pnfs
import atomic
import enstore_functions2
import file_utils

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

fullpath = enstore_functions2.fullpath


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
        #self.set_error_handler(self.handle_error)
        Trace.init(self.log_name)
	self.alive_interval = monitored_server.get_alive_interval(self.csc,
								  MY_NAME,
								  self.keys)
    """
    def handle_error(self, exc, msg, tb):
        x = tb 
        Trace.log(e_errors.ERROR, "handle pnfs agent error %s %s"%(exc, msg))
        Trace.trace(10, "%s " %(self.current_work_ticket, ))
        if self.current_work_ticket:
            try:
                Trace.trace(10, "handle error: calling transfer failed, str(msg)=%s"%(str(msg),))
                self.transfer_failed(exc, msg)
            except:
                pass
    """

    # show_state -- show internal configuration values
    def show_state(self, ticket):
        ticket['state'] = {}
        for i in self.__dict__.keys():
            ticket['state'][i] = `self.__dict__[i]`
        ticket['status'] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    def get_pnfsstat(self, ticket):
        filename = ticket['filename']
        p=pnfs.Pnfs(filename)
        try:
            ticket['statinfo']=p.get_stat()
            ticket['status'] = (e_errors.OK, None)
        except OSError, msg:
            ticket['statinfo'] = None
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.OSERROR, str(msg))
        except OSError, msg:
            ticket['statinfo'] = None
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.IOERROR, str(msg))
        self.reply_to_caller(ticket)
        return

    def get_stat(self, ticket):
        filename = ticket['filename']
        #p=pnfs.Pnfs(filename)
        try:
            ticket['statinfo']=tuple(os.stat(filename))
            ticket['status'] = (e_errors.OK, None)
        except OSError, msg:
            ticket['statinfo'] = None
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.OSERROR, str(msg))
        except OSError, msg:
            ticket['statinfo'] = None
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.IOERROR, str(msg))
        self.reply_to_caller(ticket)
        return

    def creat(self, ticket):
        filename = ticket['filename']
        mode = ticket.get('mode', None)
        p=pnfs.Pnfs(filename)
        try:
            if mode:
                p.creat(filename, mode)
            else:
                p.creat(filename)
            ticket['status'] = (e_errors.OK, None)
        except OSError, msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.OSERROR, str(msg))
        except OSError, msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.IOERROR, str(msg))
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

    def get_pinfo(self, ticket):
        pin = pnfs.Pnfs(ticket['filename'])
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
        ticket['status']   = (e_errors.OK, None)
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


    def get_file_family_wrapper(self, ticket):
        dirname = ticket['dirname']
        if ( os.path.exists(dirname) ) :
            if ( os.path.isdir(dirname) ) :
                t = pnfs.Tag(dirname)
                ticket['file_family_wrapper']=t.get_file_family_wrapper()
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

    def get_storage_group(self, ticket):
        dirname = ticket['dirname']
        if ( os.path.exists(dirname) ) :
            if ( os.path.isdir(dirname) ) :
                t = pnfs.Tag(dirname)
                ticket['storage_group']=t.get_storage_group()
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

    def create_zero_length_pnfs_files(self,ticket):
        if type(ticket['filenames']) != types.ListType:
            ticket['filenames'] = [ticket['filenames']]
        for f in ticket['filenames']:
            if type(f) == types.DictType:
                fname = f['wrapper']['pnfsFilename']
            else:
                fname = f
            try:
                Trace.log(e_errors.INFO, 'opening file %s'%fname)
                fd = atomic.open(fname, mode=0666) #raises OSError on error.

                if type(f) == types.DictType:
                    #The inode is used later on to determine if another process
                    # has deleted or removed the remote pnfs file.
                    f['wrapper']['inode'] = os.fstat(fd)[stat.ST_INO]
                    #The pnfs id is used to track down the new paths to files
                    # that were moved before encp completes.
                    f['fc']['pnfsid'] = pnfs.Pnfs(fname).get_id()
                os.close(fd)
            except OSError, msg:
                Trace.log(e_errors.ERROR, msg)
                ticket['status'] = (e_errors.OSERROR, None)
                ticket['msg'] = msg
                self.reply_to_caller(ticket)
                return
        ticket['status']   = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    def get_ninfo(self,ticket) :
        unused, ifullname, unused, ibasename = fullpath(ticket['inputfile']) #e.input[i])
        unused, ofullname, unused, unused = fullpath(ticket['outputfile']) #e.output[0])
        inlen = ticket['inlen']
        if ofullname == "/dev/null": #if /dev/null is target, skip elifs.
            pass
        elif ifullname == "/dev/zero":
            pass
        elif ( inlen > 1) or \
         (inlen == 1 and os.path.isdir(ofullname)):
            ofullname = os.path.join(ofullname, ibasename)
            unused, ofullname, unused, unused = fullpath(ofullname)
        ticket['inputfile']=ifullname
        ticket['outputfile']=ofullname
        ticket['status']   = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    def get_path(self,ticket) :
        pnfs_id  = ticket['pnfs_id']
        dirname  = ticket['dirname']
        p = pnfs.Pnfs(pnfs_id,dirname)
        ticket['path'] = p.get_path()
        ticket['status']   = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    def e_access(self,ticket) :
        path  = ticket['path']
        mode  = ticket['mode']
        rc = file_utils.e_access(path,mode)
        Trace.log(e_errors.INFO, 'e_access for file %s mode %s rc=%s'%(path,mode,rc,))
        ticket['rc']=rc
        ticket['status']   = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    def set_bit_file_id(self,ticket) :
        bfid  = ticket['bfid']
        fname = ticket['fname']
        p=pnfs.Pnfs(fname)
        p.set_bit_file_id(bfid)
        ticket['status']   = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    def get_bit_file_id(self,ticket) :
        fname = ticket['fname']
        p=pnfs.Pnfs(fname)
        ticket['bfid']=p.get_bit_file_id()
        ticket['status']   = (e_errors.OK, None)
        Trace.log(e_errors.INFO, 'get_bit_file_id %s %s'%(fname,ticket['bfid'],))
        self.reply_to_caller(ticket)
        return

    def get_id(self,ticket) :
        fname = ticket['fname']
        try:
            p=pnfs.Pnfs(fname)
            ticket['file_id']=p.get_id()
            ticket['status']   = (e_errors.OK, None)
        except OSError, msg:
            ticket['file_id']=None
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.OSERROR, str(msg))
        except IOError, msg:
            ticket['file_id']=None
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.IOERROR, str(msg))
        self.reply_to_caller(ticket)
        Trace.log(e_errors.INFO, 'get_id %s %s'%(fname,ticket['file_id'],))
        return

    def get_parent_id(self,ticket) :
        pnfsid = ticket['pnfsid']
        try:
            p=pnfs.Pnfs(pnfsid, shorcut=True)
            ticket['parent_id']=p.get_parent_id()
            ticket['status']   = (e_errors.OK, None)
        except OSError, msg:
            ticket['parent_id']=None
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.OSERROR, str(msg))
        except IOError, msg:
            ticket['parent_id']=None
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.IOERROR, str(msg))
        self.reply_to_caller(ticket)
        Trace.log(e_errors.INFO,
                  'get_parent_id pnfs %s'%(ticket['parent_id'],))
        return

    def set_xreference(self,ticket) :
        fname = ticket['pnfsFilename']
        p=pnfs.Pnfs(fname)
        p.get_bit_file_id()
        p.get_id()
        p.set_xreference(ticket['volume'],
                         ticket['location_cookie'],
                         ticket['size'],
                         ticket['file_family'],
                         ticket['pnfsFilename'],
                         ticket['volume_filepath'],
                         ticket['id'],
                         ticket['volume_fileP'],
                         ticket['bit_file_id'],
                         ticket['drive'],
                         ticket['crc'],
                         ticket['filepath'])
        ticket['status']   = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    def set_file_size(self,ticket) :
        fname = ticket['fname']
        p=pnfs.Pnfs(fname)
        p.get_bit_file_id()
        p.get_id()
        p.set_file_size(ticket['size'])
        ticket['status']   = (e_errors.OK, None)
        Trace.log(e_errors.INFO, 'set_file_size %s %s'%(fname,ticket['size'],))
        self.reply_to_caller(ticket)
        return

    def set_outfile_permissions(self,ticket) :
        work_ticket = ticket['ticket']
        if not ticket.get('copy', None):  #Don't set permissions if copy.
            set_outfile_permissions_start_time = time.time()
            #Attempt to get the input files permissions and set the output file to
            # match them.
            if work_ticket['outfile'] != "/dev/null":
                try:
                    perms = None
                    if ( os.path.exists(work_ticket['infile']) ):
                        perms = os.stat(work_ticket['infile'])[stat.ST_MODE]
                    else:
                        perms = work_ticket['wrapper']['pstat'][stat.ST_MODE]
                    os.chmod(work_ticket['outfile'], perms)
                    uid=work_ticket['wrapper'].get('uid',None)
                    gid=work_ticket['wrapper'].get('gid',None)
                    os.chown(work_ticket['outfile'],uid,gid)
                    work_ticket['status'] = (e_errors.OK, None)
                except OSError, msg:
                    Trace.log(e_errors.INFO, "chmod %s failed %s:" % \
                              (work_ticket['outfile'], msg))
                    work_ticket['status'] = (e_errors.USERERROR,
                                        "Unable to set permissions.")
#        file_utils.set_outfile_permissions(work_ticket)
        ticket['status']   = (e_errors.OK, None)
        ticket['ticket']   = work_ticket
        self.reply_to_caller(ticket)
        return

    def chmod(self,ticket) :
        fname = ticket['fname']
        mode = ticket['mode']
        p=pnfs.Pnfs(fname)
        try:
            p.chmod(mode)
            ticket['status'] = (e_errors.OK, None)
        except OSError, msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.OSERROR, str(msg))
        except IOError, msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.IOERROR, str(msg))
        self.reply_to_caller(ticket)
        return

    #Delete the file while clobbering layers.
    def utime(self,ticket):
        fname = ticket['fname']
        p=pnfs.Pnfs(fname)
        try:
            p.utime()
            ticket['status'] = (e_errors.OK, None)
        except OSError, msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.OSERROR, str(msg))
        except IOError, msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.IOERROR, str(msg))
        self.reply_to_caller(ticket)
        return

    #Delete the file while clobbering layers.
    def rm(self,ticket):
        fname = ticket['fname']
        p=pnfs.Pnfs(fname)
        try:
            p.rm()
            ticket['status'] = (e_errors.OK, None)
        except OSError, msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.OSERROR, str(msg))
        except IOError, msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.IOERROR, str(msg))
        self.reply_to_caller(ticket)
        return
        
    #Delete the file.
    def remove(self,ticket):
        fname = ticket['fname']
        try:
            os.remove(fname)
            ticket['status'] = (e_errors.OK, None)
        except OSError, msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.OSERROR, str(msg))
        except IOError, msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.IOERROR, str(msg))
        self.reply_to_caller(ticket)
        return

    def is_pnfs_path(self, ticket):
        fname = ticket['fname']
        ticket['rc'] = pnfs.is_pnfs_path(fname)
        ticket['status'] = (e_errors.OK, None)
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
