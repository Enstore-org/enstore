#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# system imports
import os
import sys
import signal
import Trace
import errno
try:
    import threading
    import thread
    thread_support=1
except ImportError:
    thread_support=0


# enstore modules
import configuration_client
import file_clerk_client
import enstore_functions2
import e_errors
import pnfs_agent_client
import file_utils
import pnfs

class Container:
    pass

thread_specific_data = {}  #global value with each item for a different thread
deletion_list_lock = threading.Lock()

#Build thread specific data.
def get_deletion_lists():
    if thread_support:
        tid = thread.get_ident() #Obtain unique identifier.
    else:
        tid = 1

    rtn_tsd = thread_specific_data.get(tid)

    try:
        #Cleanup
        for tid, tsd in thread_specific_data.items():
            #Loop though all of the active threads searching for
            # the thread specific data (tsd) that it relates to.
            for a_thread in threading.enumerate():
                if not hasattr(a_thread, "tid"):
                    #If there is no tid attribute, it hasn't used
                    # this udp_client and thus we don't care.
                    continue
                if a_thread.tid == tid:
                    #If the thread is still active, don't cleanup.
                    continue

                del thread_specific_data[tid]
    except (KeyboardInterrupt, SystemExit):
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except:
        exc, msg = sys.exc_info()[:2]
        try:
            sys.stderr.write("%s: %s\n" % (str(exc), str(msg)))
            sys.stderr.flush()
        except IOError:
            pass
        pass
             
               
            
    if not rtn_tsd:
        thread_specific_data[tid] = Container()
        thread_specific_data[tid].bfids = []
        thread_specific_data[tid].files = []

        if thread_support:
            #There is no good way to store which thread this tsd was
            # create for.  It used to do the following.
            #     tsd.thread = threading.currentThread()
            # But this turns out to be a resource leak by creating a
            # cyclic reference.  Thus, this hack was devised to track
            # them from the other direction; namely knowing the thread
            # identify the tsd in the self.tsd dict that it relates to.
            threading.currentThread().tid = tid

        rtn_tsd = thread_specific_data[tid]
            
    return rtn_tsd


def register(filename):
    if filename == '/dev/null':
        return

    deletion_list_lock.acquire()

    _deletion_list = get_deletion_lists().files
    if filename not in _deletion_list:
        _deletion_list.append(filename)

    deletion_list_lock.release()    

def register_bfid(bfid):
    deletion_list_lock.acquire()

    _deletion_list_bfids = get_deletion_lists().bfids
    if bfid not in _deletion_list_bfids:
        _deletion_list_bfids.append(bfid)

    deletion_list_lock.release()

def unregister(filename):
    if filename == '/dev/null':
        return

    deletion_list_lock.acquire()
    
    _deletion_list = get_deletion_lists().files
    if filename in _deletion_list:
        _deletion_list.remove(filename)

    deletion_list_lock.release()

def unregister_bfid(bfid):
    deletion_list_lock.acquire()
    
    _deletion_list_bfids = get_deletion_lists().bfids
    if bfid in _deletion_list_bfids:
        _deletion_list_bfids.remove(bfid)
    
    deletion_list_lock.release()


def delete():

    deletion_list_lock.acquire()

    _deletion_list = get_deletion_lists().files
    _deletion_list_bfids = get_deletion_lists().bfids

    if not _deletion_list and not _deletion_list_bfids:
        deletion_list_lock.release() # Avoid deadlocks.
        return

    # get a configuration server
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = configuration_client.ConfigurationClient((config_host, config_port))
    
    # Delete registered files.
    for f in _deletion_list:
        Trace.log(e_errors.INFO, "Performing file cleanup for file: %s" % (f,))

        if os.path.exists(f):
            #We need to obtain the parent directory for .(access)() file names.
            # The unlink() fails (without an error) if the parent directory is
            # not included in the path.  This also requires that the basename
            # not be a .(access)() name, so convert its real name.
            if pnfs.is_access_name(f):
                pnfsid = os.path.basename(f)[10:-1]
                use_f = os.path.join(pnfs.get_directory_name(f),
                                     pnfs.Pnfs().get_nameof(pnfsid))
            else:
                use_f = f
            try:
                os.unlink(use_f)
                _deletion_list.remove(f) #Remove from the list.
            except OSError, msg:
                if msg.errno in [errno.EPERM, errno.EACCES] \
                   and os.getuid() == 0 and os.geteuid() != 0:
                    #Reset the euid and egid.
                    directory = pnfs.get_directory_name(f)
                    file_utils.match_euid_egid(directory)
                
                    try:
                        os.unlink(f)
                        _deletion_list.remove(f) #Remove from the list.
                    except OSError, msg2:
                        message = "Can not delete file %s. (%s)" % (f, msg2)
                        Trace.log(e_errors.ERROR, message)
                        try:
                            sys.stderr.write("%s%s" % (message, "\n"))
                            sys.stderr.flush()
                        except IOError:
                            pass

                    #Release the lock.
                    file_utils.end_euid_egid()
                else:
                    message = "Can not delete file %s. (%s)" % (f, msg)
                    Trace.log(e_errors.ERROR, message)
                    try:
                        sys.stderr.write("%s%s" % (message, "\n"))
                        sys.stderr.flush()
                    except IOError:
                        pass

        else:
            pnfs_agent_answer = csc.get("pnfs_agent", 5, 5)
            #We need to check if the optional pnfs_agent is even configured.
            # If it is, then we can continue to try and remove the file.
            if e_errors.is_ok(pnfs_agent_answer):
                pac = pnfs_agent_client.PnfsAgentClient(csc)
                #If pac.remove() had the protections on the pnfs agent side
                # to make sure only pnfs files were deleted this is_pnfs_path()
                # check would not be necessary.
                if pac.is_pnfs_path(f):
                    pac.remove(f)
                    _deletion_list.remove(f) #Remove from the list.
            
    # Delete registered bfids.
    for b in _deletion_list_bfids:
        Trace.log(e_errors.INFO, "Performing bfid cleanup for: %s" % (b,))
        try:
            fcc = file_clerk_client.FileClient(csc, b)
            fcc.set_deleted("yes")
        except:
            Trace.log(e_errors.ERROR,
                      "Can not delete bfid %s from database.\n" % (b,))
            try:
                sys.stderr.write("Can not delete bfid %s from database.\n" % (b,))
                sys.stderr.flush()
            except IOError:
                    pass
            
    deletion_list_lock.release()

            
def signal_handler(sig, frame):

    try:
        if sig not in [signal.SIGTERM, signal.SIGINT, signal.SIGQUIT]:
            sys.stderr.write("Signal caught at: %s line %d\n" %
                             (frame.f_code.co_filename, frame.f_lineno))
            sys.stderr.flush()
    except (OSError, IOError, TypeError):
        exc, msg = sys.exc_info()[:2]
        try:
            sys.stderr.write("%s: %s\n" % (str(exc), str(msg)))
            sys.stderr.flush()
        except IOError:
            pass
    
    try:
        sys.stderr.write("Caught signal %s, exiting.\n" % (sig,))
        sys.stderr.flush()
    except IOError:
        pass

    Trace.log(e_errors.ERROR, "Caught signal %s, exiting.\n" % (sig,))

    quit(1)

def setup_signal_handling():
    
    #Handle all signals not in the known skip list.
    for sig in range(1, signal.NSIG):
        if sig not in (signal.SIGTSTP, signal.SIGCONT,
                       signal.SIGCHLD, signal.SIGWINCH):
            try:
                signal.signal(sig, signal_handler)
            except RuntimeError:
                pass
            except (ValueError, TypeError):
                try:
                    sys.stderr.write("Setting signal %s to %s failed.\n" %
                                     (sig, signal_handler))
                    sys.stderr.flush()
                except IOError:
                    pass

#Simply ignoring the pychecker warning really isn't a great solution.  A
# different name other than "quit" sould really be used.  It is just used
# in so many places.
__pychecker__ = "no-shadowbuiltin"
def quit(exit_code=1):
    
    #Perform cleanup.
    delete()

    #The os._exit() call below does not flush out the contents in file
    # descriptor buffers.  To get stdout and stderr to do this, we must do so
    # explicitly, before calling os._exit().
    try:
        sys.stdout.flush()
        sys.stderr.flush()
    except IOError:
        pass

    #Exit in a unclean way.
    ### Note MWZ 2-26-2004: There is (likely) a reason why this has always
    ### been done with os._exit(), but I don't know what it is...
    os._exit(exit_code)
