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
import types
try:
    import threading
    #import thread
    thread_support = 1
except ImportError:
    thread_support = 0


# enstore modules
import configuration_client
import file_clerk_client
import enstore_functions2
import e_errors
import file_utils
import namespace

# global value with each item for a different thread
thread_specific_data = threading.local()
# global locks for the thread_specific_data
deletion_list_lock = threading.Lock()

# boolean to catch a signal handler called while running another signal
# handler.
signal_in_progress = False
# lock to protect kill_in_progress
sip_lock = threading.Lock()

# Build thread specific data.  get_deletion_lists() should only be called
# from functions that have acquired the deletion_list_lock lock.


def get_deletion_lists():
    global thread_specific_data

    if not hasattr(thread_specific_data, "bfids"):
        thread_specific_data.bfids = []
    if not hasattr(thread_specific_data, "files"):
        thread_specific_data.files = []

    return thread_specific_data

# get_deletion_lists() should only be called from functions that have
# acquired the deletion_list_lock lock.


def clear_deletion_lists():
    global thread_specific_data

    thread_specific_data.bfids = []
    thread_specific_data.files = []


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

    # Acquire the list of things to delete.
    del_list = get_deletion_lists()
    _deletion_list = del_list.files
    _deletion_list_bfids = del_list.bfids

    if not _deletion_list and not _deletion_list_bfids:
        deletion_list_lock.release()  # Avoid deadlocks.
        return

    # get a configuration server
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = configuration_client.ConfigurationClient((config_host, config_port))

    # Delete registered files.
    for f in _deletion_list:
        Trace.log(e_errors.INFO, "Performing file cleanup for file: %s" % (f,))

        if os.path.exists(f):
            # We need to obtain the parent directory for .(access)() file names.
            # The unlink() fails (without an error) if the parent directory is
            # not included in the path.  This also requires that the basename
            # not be a .(access)() name, so convert its real name.
            fs = namespace.StorageFS(f)
            try:
                fs.rm(f)
                _deletion_list.remove(f)  # Remove from the list.
            except BaseException:
                Trace.log(e_errors.ERROR,
                          "Can not delete file %s from fs.\n" % (f,))

    # Delete registered bfids.
    for b in _deletion_list_bfids:
        Trace.log(e_errors.INFO, "Performing bfid cleanup for: %s" % (b,))
        try:
            fcc = file_clerk_client.FileClient(csc, b)
            fcc.set_deleted("yes")
        except BaseException:
            Trace.log(e_errors.ERROR,
                      "Can not delete bfid %s from database.\n" % (b,))
            try:
                sys.stderr.write(
                    "Can not delete bfid %s from database.\n" %
                    (b,))
                sys.stderr.flush()
            except IOError:
                pass

    deletion_list_lock.release()


def signal_handler(sig, frame):
    global signal_in_progress

    if signal_in_progress:
        try:
            sys.stderr.write("Ignoring signal %s, previous signal handler "
                             "still running.\n" % (sig,))
            sys.stderr.flush()
        except IOError:
            pass
        # Go back to the first signal handler.
        return

    # Try and solve the problem where a second signal can be received while
    # the signal handler from the first signal is still running.
    signal_in_progress = True

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

    try:
        Trace.log(e_errors.ERROR, "Caught signal %s, exiting.\n" % (sig,))
    except (OSError, IOError):
        pass

    # A lot of utilities return different values when a signal terminates them.
    quit(128 + sig)


def setup_signal_handling():

    # This block of code is necessary on systems that use signals internally
    # within the C libraries.  It finds the highest user defined signal.
    # Then it creates a list of all signals between the highest signal
    # and the smallest realtime signal.
    #
    # This is important on Linux, which uses signals 32 (SIGCANCEL & SIGTIMER)
    # and 33 (SIGSETXID) to handle inter-thread processing of per-process
    # resources.  SIGSETXID is used to coordinate all threads in a process
    # changing thier UID and GID (and other things), which is a process
    # resource and not a per-thread resource.  These extra signals are not
    # defined in signal.h, and thusly not in the singal module; which is why
    # all of this extra coding is necesary to detect if there are even any
    # on a given system.  On Linux 2.6 the greatest normal signal is 31 and
    # SIGRTMIN is 34, which is why 32 and 33 need to be handled special.
    max_regular_signal = 0
    for key in dir(signal):
        value = getattr(signal, key)
        if isinstance(value, int) and \
           key[0:3] == "SIG" and key not in ("SIGRTMAX", "SIGRTMIN"):
            if value > max_regular_signal:
                max_regular_signal = value
    # Create the list from the range.
    SIGRTMIN = getattr(signal, "SIGRTMIN", None)
    if SIGRTMIN:
        sig_leave_alone_list = range(max_regular_signal + 1, SIGRTMIN)
    else:
        # We need this for non-Linux systems that don't define SIGRTMIN.
        sig_leave_alone_list = []

    # This is a known list of signals to leave thier default handler in place.
    sig_leave_alone_list.append(signal.SIGTSTP)
    sig_leave_alone_list.append(signal.SIGCONT)
    sig_leave_alone_list.append(signal.SIGCHLD)
    sig_leave_alone_list.append(signal.SIGWINCH)
    sig_leave_alone_list.append(signal.SIGPIPE)  # Use python's default.

    # Handle all signals not in the known skip list.
    for sig in range(1, signal.NSIG):
        if sig not in sig_leave_alone_list:
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


# Simply ignoring the pychecker warning really isn't a great solution.  A
# different name other than "quit" sould really be used.  It is just used
# in so many places.
__pychecker__ = "no-shadowbuiltin"


def quit(exit_code=1):

    # Perform cleanup.
    delete()

    # The os._exit() call below does not flush out the contents in file
    # descriptor buffers.  To get stdout and stderr to do this, we must do so
    # explicitly, before calling os._exit().
    try:
        sys.stdout.flush()
        sys.stderr.flush()
    except IOError:
        pass

    # Exit in a unclean way.
    # Note MWZ 2-26-2004: There is (likely) a reason why this has always
    # been done with os._exit(), but I don't know what it is...
    os._exit(exit_code)
