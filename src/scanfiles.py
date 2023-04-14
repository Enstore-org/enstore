#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# system imports
import os
import sys
import stat
import string
import time
import errno
import types
import threading
import thread
import copy
import gc
import signal
import re


# enstore modules
import bfid_util
import info_client
import configuration_client
import option
import namespace
import volume_family
import e_errors
import Trace
import enstore_constants
import checksum
import find_pnfs_file
import enstore_functions2
import enstore_functions3
import file_utils

class ThreadWithResult(threading.Thread):
    def __init__(self, *pargs, **kwargs):
        threading.Thread.__init__(self, *pargs, **kwargs)
        self.result = None
        self.is_joinable = False

    def get_args(self):
        return copy.deepcopy(self._Thread__args)

    def get_result(self):
        try:
            return copy.deepcopy(self.result)
        except:
            return None

    def run(self):
        # do my stuff here that generates results
        try:
            self.result = apply(self._Thread__target, self._Thread__args)
        except (KeyboardInterrupt, SystemExit):
            pass
        except:
            exc, msg, tb = sys.exc_info()
            import traceback
            traceback.print_tb(tb)
            print str(exc), str(msg)

        #Tell the main thread that this thread is done.
        self.is_joinable = True

        # and now the thread exits

    #The threading.Thread.join() function doesn't handle singals while waiting.
    # Thus, this version overrides that behavior.
    def join(self, timeout=None):
        if self.is_joinable:
            threading.Thread.join(self, timeout)
        else:
            start_time = time.time()
            while timeout == None or time.time() - start_time < timeout:
                #We need to check for is_joinable.  If we call join() while
                # the thread is still running the call to join() will hang
                # until the thread exists.  This normally wouldn't be a
                # problem, but in python this allows the process to ignore
                # signals, like the one generated from a Ctrl-C.
                if self.is_joinable:

                    threading.Thread.join(self)

                    #If we joined the thread, leave the loop.
                    break

                else:
                    time.sleep(0.1)

    def reset(self, *pargs):
        self._Thread__args = pargs
        self._Thread__started = 0
        self._Thread__stopped = 0
        self.result = None


infc = None
vol_info = {} #File Family and Library Manager cache.
lm = [] #Library Manager cache.
ONE_DAY = 24*60*60
ts_check = []
stop_threads_lock=threading.Lock()
threads_stop = False
alarm_lock=threading.Lock()

#Collect all child threads.  Wait for them to exit if necessary.
def cleanup_threads():
    while len(ts_check) > 0:
        for i in range(len(ts_check)):
            ts_check[i].join()
            result = ts_check[i].get_result()
            try:
                err_j, warn_j, info_j = result #Why do we do this?
            except TypeError:
                pass #???
            del ts_check[i]

            #If we joined a thread, go back to the top of the while.
            # If we don't we will have a discrepancy between indexes
            # from before the "del" and after the "del" of ts_check[i].
            break

# union(list_of_sets)
###copied from file_clerk_client.py
def union(s):
    res = []
    for i in s:
        for j in i:
            if not j in res:
                res.append(j)
    return res

#For cleanup_objects() to report problems.
old_list = []
old_len  = 0
def cleanup_objects():
    global old_list
    global old_len

    #Get current information on the number of objects currently consuming
    # resources.
    new_list = gc.get_objects()
    new_len = len(new_list)
    del new_list[:]

    #Force garbage collection while the display is off while awaiting
    # initialization.
    gc.collect()
    del gc.garbage[:]
    gc.collect()
    uncollectable_count = len(gc.garbage)
    del gc.garbage[:]

    #First report what the garbage collection algorithm says...
    if uncollectable_count > 0:
        Trace.trace(0, "UNCOLLECTABLE COUNT: %s" % uncollectable_count)

    #Then (starting with the second pass) report the object count difference.
    if old_len == 0:
        old_len = new_len #Only set this on the first pass.
    else:
        if new_len - old_len > 0:
            try:
                sys.stderr.write("NEW COUNT DIFFERENCE: %s - %s = %s\n"
                                 % (new_len, old_len, new_len - old_len))
                sys.stderr.flush()
            except IOError:
                pass
            """
            i = 0
            for item in gc.get_objects():
                print i
                if item not in old_list:
                    print str(item)[:77]
                    i = i + 1
            """
        if new_len - old_len >= 100:
            #Only return true if
            return True

    return None


def usage():
    print "usage: %s [path [path2 [path3 [ ... ]]]]"%(sys.argv[0])
    print "usage: %s --help"%(sys.argv[0])
    print "usage: %s --infile file"%(sys.argv[0])

def error(s):
    print s, '... ERROR'

def warning(s):
    print s, '... WARNING'

print_lock = threading.Lock()
def errors_and_warnings(fname, error, warning, information):

    if type(error) != types.ListType or \
           type(warning) != types.ListType or \
           type(information) != types.ListType:
        return

    print_lock.acquire()

    print fname +' ...',
    # print warnings
    for i in warning:
        print i + ' ...',
    # print errors
    for i in error:
        print i + ' ...',
    # print information
    for i in information:
        print i + ' ...',
    if error:
        print 'ERROR'
    elif warning:
        print 'WARNING'
    elif information:
        print 'OK'
    else:
        print 'OK'

    print_lock.release()

###############################################################################

def get_enstore_pnfs_path(filename):

    absolute_path = os.path.abspath(filename)

    #This is not automount safe.
    if absolute_path.find("/pnfs/fs/usr/") >= 0:
        return absolute_path.replace("/pnfs/fs/usr/", "/pnfs/", 1)
    elif absolute_path.find("/pnfs/fnal.gov/usr/") >= 0:
        return absolute_path.replace("/pnfs/fnal.gov/usr/", "/pnfs/", 1)
    elif absolute_path.find("/pnfs/") >= 0:
        return absolute_path.replace("/pnfs/", "/pnfs/", 1)
    else:
        return absolute_path

def get_dcache_pnfs_path(filename):

    absolute_path = os.path.abspath(filename)

    #This is not automount safe.
    if absolute_path.find("/pnfs/fs/usr/") >= 0:
        return absolute_path
    elif absolute_path.find("/pnfs/fnal.gov/usr/") >= 0:
        return absolute_path.replace("/pnfs/fnal.gov/usr/", "/pnfs/fs/usr/", 1)
    elif absolute_path.find("/pnfs/") >= 0:
        return absolute_path.replace("/pnfs/", "/pnfs/fs/usr/", 1)
    else:
        return absolute_path

###############################################################################

def is_new_database(d):

    dbname = os.path.join(d, ".(get)(database)")
    dbparent = os.path.join(os.path.dirname(d), ".(get)(database)")

    try:
        dbnamefile = open(dbname)
        dbnameinfo = dbnamefile.readline().strip()
        dbnamefile.close()
    except IOError:
        #This should never happen for PNFS.  Will always happen for Chimera.
        return False

    try:
        dbparentfile = open(dbparent)
        dbparentinfo = dbparentfile.readline().strip()
        dbparentfile.close()
    except IOError:
        #In order to get this error, we need to be at the top of the
        # filesystem.  Just return False because there should be no need to
        # worry about another starting point.
        return False

    if dbnameinfo == dbparentinfo:
        return False

    return True

access_match = re.compile("\.\(access\)\([0-9A-Fa-f]+\)")
def is_access_name(filepath):
    global access_match
    #Determine if it is an ".(access)()" name.
    if re.search(access_match, os.path.basename(filepath)):
        return True

    return False

###############################################################################

def layer_file(f, n):
    pn, fn = os.path.split(f)
    if is_access_name(fn):
        return os.path.join(pn, "%s(%d)" % (fn, n))
    else:
        return os.path.join(pn, ".(use)(%d)(%s)" % (n, fn))

def id_file(f):
    #We need to be careful that a .(access)() file does not get passed here.
    pn, fn = os.path.split(f)
    return os.path.join(pn, ".(id)(%s)" % (fn, ))

def parent_file(f, sfsid = None):
    pn, fn = os.path.split(f)
    if sfsid:
        return os.path.join(pn, ".(parent)(%s)" % (sfsid))
    if is_access_name(f):
        sfsid = fn[10:-1]
        return os.path.join(pn, ".(parent)(%s)" % (sfsid))
    else:
        fname = id_file(f)
        f = open(fname)
        sfsid = f.readline()
        f.close()
        return os.path.join(pn, ".(parent)(%s)" % (sfsid))

def access_file(dn, sfsid):
    return os.path.join(dn, ".(access)(%s)" % (sfsid))

def database_file(directory):
    return os.path.join(directory, ".(get)(database)")

###############################################################################

def get_database(f):
    #err = []
    #warn = []
    #info = []

    db_a_dirpath = namespace.get_directory_name(f)
    database_path = database_file(db_a_dirpath)

    try:
        database = get_layer(database_path)[0].strip()
    except (OSError, IOError):
        database = None
        #if detail.errno in [errno.EACCES, errno.EPERM]:
        #    err.append('no read permissions for .(get)(database)')
        #elif detail.args[0] in [errno.ENOENT, errno.ENOTDIR]:
        #    pass
        #else:
        #    err.append('corrupted .(get)(database) metadata')

    return database


def get_layer(layer_filename):
    fl = file_utils.open(layer_filename)
    layer_info = fl.readlines()
    fl.close()
    return layer_info

def get_layer_1(f):
    err = []
    warn = []
    info = []

    # get bfid from layer 1
    try:
        layer1 = get_layer(layer_file(f, 1))
    except (OSError, IOError), detail:
        layer1 = None
        if detail.errno in [errno.EACCES, errno.EPERM]:
            err.append('no read permissions for layer 1')
        elif detail.args[0] in [errno.ENOENT, errno.EISDIR]:
            pass
        else:
            err.append('corrupted layer 1 metadata')

    try:
        bfid = layer1[0].strip()
    except:
        bfid = ""

    if layer1 and len(layer1) > 1:
        err.append("extra layer 1 lines detected")

    return bfid, (err, warn, info)

## Take the return from the regualar expression and check to make sure that
## everything is okay.
#
#all_matches is the return value from re.findall().
#check_type_string is the name used for error strings.
def __l2_match_check(all_matches, check_type_string):
    err = []
    warn = []
    info = []

    if len(all_matches) > 1:
        for current_match in all_matches[1:]:
            if current_match != all_matches[0]:
                err.append("multiple layer 2 %ss found (%s, %s)" % \
                           (check_type_string, all_matches[0], current_match))
                #Set the CRC to something other than None, to
                # supress the "no layer 2 crc" error reported
                # elsewhere if the CRC is None.
                rtn = ""
                break
        else:
            warn.append("layer 2 %ss repeated %s times" % \
                        (check_type_string, len(all_matches),))
            #Even though we have too many CRCs they all match, so
            # we can return the CRC.
            rtn = all_matches[0]
    elif len(all_matches) == 1:
        rtn = all_matches[0]
    else:
        rtn = None

    return rtn, (err, warn, info)

def get_layer_2(f):

    err = []
    warn = []
    info = []

    # get dcache info from layer 2
    try:
        layer2 = get_layer(layer_file(f, 2))
    except (OSError, IOError), detail:
        layer2 = None
        if detail.errno in [errno.EACCES, errno.EPERM]:
            err.append('no read permissions for layer 2')
        elif detail.args[0] in [errno.ENOENT, errno.EISDIR]:
            pass
        else:
            err.append('corrupted layer 2 metadata')

    l2 = {}
    (err_hsm, warn_hsm, info_hsm) = ([], [], [])
    (err_crc, warn_crc, info_crc) = ([], [], [])
    (err_size, warn_size, info_size) = ([], [], [])
    if layer2:
        try:
            l2['line1'] = layer2[0].strip()
        except IndexError:
            l2['line1'] = None

        try:
            line2 = layer2[1].strip()
        except IndexError:
            line2 = ""

        try:
            hsm_match = re.compile("h=(no|yes)")
            all_hsms = hsm_match.findall(line2)
            l2['hsm'], (err_hsm, warn_hsm, info_hsm) = \
               __l2_match_check(all_hsms, "HSM")
            #l2['hsm'] = hsm_match.search(line2).group().split("=")[1]
        except AttributeError:
            l2['hsm'] = None

        try:
            crc_match = re.compile("c=1:[a-zA-Z0-9]{1,8}")
            all_crcs = crc_match.findall(line2)
            crc, (err_crc, warn_crc, info_crc) = \
               __l2_match_check(all_crcs, "CRC")
            if crc:
                l2['crc'] = long(crc.split(":")[1], 16)
            #l2['crc'] = long(crc_match.search(line2).group().split(":")[1], 16)
        except (AttributeError, ValueError):
            l2['crc'] = None

        try:
            size_match = re.compile("l=[0-9]+")
            all_sizes = size_match.findall(line2)
            size, (err_size, warn_size, info_size) = \
               __l2_match_check(all_sizes, "size")
            l2['size'] = long(size.split("=")[1])
            #l2['size'] = long(size_match.search(line2).group().split("=")[1])
        except AttributeError:
            l2['size'] = None

        l2['pools'] = []
        for item in layer2[2:]:
            l2['pools'].append(item.strip())

    err = union([err, err_hsm, err_crc, err_size])
    warn = union([warn, warn_hsm, warn_crc, warn_size])
    info = union([info, info_hsm, info_crc, info_size])
    return l2, (err, warn, info)

def get_layer_4(f):

    err = []
    warn = []
    info = []

    # get xref from layer 4 (?)
    try:
        layer4 = get_layer(layer_file(f, 4))
    except (OSError, IOError), detail:
        layer4 = None
        if detail.errno in [errno.EACCES, errno.EPERM]:
            err.append('no read permissions for layer 4')
        elif detail.args[0] in [errno.ENOENT, errno.EISDIR]:
            pass
        else:
            err.append('corrupted layer 4 metadata')

    l4 = {}
    if layer4:
        try:
            l4['volume'] = layer4[0].strip()
        except IndexError:
            pass
        try:
            l4['location_cookie'] = layer4[1].strip()
        except IndexError:
            pass
        try:
            l4['size'] = layer4[2].strip()
        except IndexError:
            pass
        try:
            l4['file_family'] = layer4[3].strip()
        except IndexError:
            pass
        try:
            l4['original_name'] = layer4[4].strip()
        except IndexError:
            pass
        # map file no longer used
        try:
            l4['pnfsid'] = layer4[6].strip()
        except IndexError:
            pass
        # map pnfsid no longer used
        try:
            l4['bfid'] = layer4[8].strip()
        except IndexError:
            pass
        try:
            l4['drive'] = layer4[9].strip() #optionally present
        except IndexError:
            pass
        try:
            l4['crc'] = layer4[10].strip() #optionally present
        except IndexError:
            pass

	MAX_L4_LINES = 11
        if len(layer4) > MAX_L4_LINES:
            err.append("extra layer 4 lines detected")

    return l4, (err, warn, info)

"""
def get_layers(f):
    bfid, (err, warn, info) = get_layer_1(f)
    layer4, (err1, warn1, info1) = get_layer_4(f)

    return (bfid, layer4), (err + err1, warn + warn1, info + info1)
"""

def verify_filedb_info(fr):
    err = []
    warn = []
    info = []

    if type(fr) != types.DictType:
        err.append("%s not a dictionary" % (fr,))
        return (err, warn, info)

    if not fr.has_key("status"):
        fr['status'] = (e_errors.OK, None)

    if e_errors.is_ok(fr):
        if fr.get('deleted', None) == "no":  # active file
            # Look for missing file database information.
            filedb_pnfs_name0 = fr.get('pnfs_name0', "")
            if filedb_pnfs_name0 in ["", None, "None"]:
                err.append('no filename in db')
            filedb_pnfsid = fr.get('pnfsid', "")
            if filedb_pnfsid in ["", None, "None"]:
                err.append('no pnfs id in db')
            elif not namespace.is_id(filedb_pnfsid):
                err.append('invalid pnfs id in db')
    elif fr['status'][0] == e_errors.NO_FILE:
        err.append('not in db')
    else:  #elif not e_errors.is_ok(fr):
        err.append('file db error (%s)' % (fr['status'],))

    return (err, warn, info)

def get_filedb_info(bfid):

    err = []
    warn = []
    info = []

    if not bfid:
        return None, (err, warn, info)

    # Get file database information.
    fr = infc.bfid_info(bfid)
    (err, warn, info) = verify_filedb_info(fr)

    return fr, (err, warn, info)

def verify_volumedb_info(vr):
    err = []
    warn = []
    info = []

    if type(vr) != types.DictType:
        err.append("%s not a dictionary" % (vr,))
        return (err, warn, info)

    if not vr.has_key("status"):
        vr['status'] = (e_errors.OK, None)

    if e_errors.is_ok(vr):
        pass
    elif vr['status'][0] == e_errors.NOVOLUME:
        err.append('volume not in db')
    else:  #elif not e_errors.is_ok(fr):
        err.append('volume db error (%s)' % (vr['status'],))

    return (err, warn, info)

def get_volumedb_info(volume):

    err = []
    warn = []
    info = []

    if not volume:
        return None, (err, warn, info)

    # file_family and library
    try:
        #Get the volume specific information.
        if vol_info.has_key(volume):
            vr = vol_info[volume]

        else:
            vr = infc.inquire_vol(volume)
            (err, warn, info) = verify_volumedb_info(vr)

            if not err:
                vol_info[volume] = vr
                vol_info[volume]['storage_group'] = \
                     volume_family.extract_storage_group(vr['volume_family'])
                vol_info[volume]['file_family'] = \
                     volume_family.extract_file_family(vr['volume_family'])
                vol_info[volume]['wrapper'] = \
                     volume_family.extract_wrapper(vr['volume_family'])

                try:
                    #We don't need these at this time.  For the 10,000s of
                    # volumes we have, we can shrink the memory footprint
                    # of the running scan.
                    del vol_info[volume]['comment']
                    del vol_info[volume]['declared']
                    del vol_info[volume]['blocksize']
                    del vol_info[volume]['sum_rd_access']
                    del vol_info[volume]['sum_mounts']
                    del vol_info[volume]['capacity_bytes']
                    del vol_info[volume]['status']
                    del vol_info[volume]['non_del_files']
                    del vol_info[volume]['sum_wr_err']
                    del vol_info[volume]['sum_wr_access']
                    del vol_info[volume]['sum_rd_err']
                    del vol_info[volume]['first_access']
                    del vol_info[volume]['last_access']
                    del vol_info[volume]['remaining_bytes']
                    del vol_info[volume]['modification_time']
                    del vol_info[volume]['write_protected']
                    del vol_info[volume]['si_time']
                    del vol_info[volume]['eod_cookie']
                except KeyError:
                    pass

    except (TypeError, ValueError, IndexError, AttributeError):
        err.append('no or corrupted volume')

    return vr, (err, warn, info)

def get_stat(f):
    __pychecker__="unusednames=i"

    err = []
    warn = []
    info = []

    #Since alarm() signals are only recieved by the main thread (python
    # restriction), and only one SIGALRM exists for all threads, we can
    # have them all share using locks in this stat() abstraction function.
    #### Unfortunatly, as of python 2.4.3 there is a bug with signals in
    #### multithreaded python programs.
    #The reason for attempting to use alarm() at all, is that it has been
    # found that the in memory copy of the database in the dbserver process
    # can become corrupted.  When it does, the PNFS filesystems responds
    # with garbage entries until it hangs.  Thus, the thought of using
    # alarm() to address the problem.
    ##alarm_lock.acquire()
    ##signal.alarm(10)

    ### We need to try a few times.  There are situations where the server
    ### is busy and the lack of responce looks like a 'does not exist' responce.
    ### This can lead 'invalid directory entry' situation, but in reality
    ### it is a false negative.

    #Do one stat() for each file instead of one for each os.path.isxxx() call.
    for i in range(2):
        try:
            f_stats = file_utils.get_stat(f, use_lstat = True)

            ##signal.alarm(0)
            ##alarm_lock.release()

            #On success return immediatly.
            return f_stats, (err, warn, info)
        except OSError, msg:
            if msg.args[0] in [errno.EBUSY, errno.ENOENT]:
                time.sleep(0.1) #Sleep for a tenth of a second.
            else:
                break

    ##signal.alarm(0)
    ##alarm_lock.release()

    if msg.errno == errno.ENOENT:

        #Before blindly returning this error, first check the directory
        # listing for this entry.  A known 'ghost' file problem exists
        # and this is how to find out.
        try:
            directory, filename = os.path.split(f)
            dir_list = file_utils.listdir(directory)
            if filename in dir_list:
                #We have this special error situation.
                err.append("invalid directory entry")
                return None, (err, warn, info)
        except OSError:
            pass

        err.append("does not exist")
        return None, (err, warn, info)

    if msg.errno == errno.EACCES or msg.errno == errno.EPERM:
        #If we get here we still could not stat the file.
        err.append("permission error")
        return None, (err, warn, info)

    err.append(os.strerror(msg.errno))
    return None, (err, warn, info)

    #return f_stats, (err, warn, info)

def get_sfsid(f):

    err = []
    warn = []
    info = []

    if is_access_name(f):
        sfsid = os.path.basename(f)[10:-1]
        return sfsid, (err, warn, info)

    #Get the id of the file or directory.
    try:
        fname = id_file(f)
        f = file_utils.open(fname)
        sfs_id = f.readline().strip()
        f.close()
    except(OSError, IOError), detail:
        sfs_id = None
        if not detail.errno == errno.ENOENT or not os.path.ismount(f):
            err.append("unable to obtain storage file system ID")

    return sfs_id, (err, warn, info)

def get_parent_id(f):

    err = []
    warn = []
    info = []

    #Get the parent id.
    try:
        dname = parent_file(f)
        f = open(dname)
        parent_id = f.readline().strip()
        f.close()
    except(OSError, IOError), detail:
        parent_id = None
        if not detail.errno == errno.ENOENT or not os.path.ismount(f):
            err.append("unable to obtain directory's parent id")

    return parent_id, (err, warn, info)

def get_parent_dir_id(f):

    err = []
    warn = []
    info = []

    #Get the id of the parent directory.
    try:
        dname = id_file(os.path.dirname(f))
        f = open(dname)
        parent_dir_id = f.readline().strip()
        f.close()
    except (OSError, IOError), detail:
        parent_dir_id = None
        if not detail.errno == errno.ENOENT or \
               (not os.path.ismount(f) \
                and not os.path.ismount(os.path.dirname(f))):
            err.append("unable to obtain parent directory's id")

    return parent_dir_id, (err, warn, info)

def get_parent_ids(f):

    parent_id, (err, warn, info) = get_parent_id(f)
    parent_dir_id, (err1, warn1, info1) = get_parent_dir_id(f)

    return (parent_id, parent_dir_id), (err + err1, warn + warn1, info + info1)

def get_all_ids(f):
    parent_id, (err, warn, info) = get_parent_id(f)
    parent_dir_id, (err1, warn1, info1) = get_parent_dir_id(f)
    sfs_id, (err2, warn2, info2) = get_sfsid(f)

    return (sfs_id, parent_id, parent_dir_id), \
           (err + err1 + err2, warn + warn1 + warn2, info + info1 + info2)

###############################################################################

###############################################################################

def check(f, f_stats = None):

    #We need to know if we should stop now.  Otherwise the main thread
    # will wait for child threads to finish.
    stop_threads_lock.acquire()
    if threads_stop:
        stop_threads_lock.release()
        thread.exit()
    stop_threads_lock.release()

    err = []
    warn = []
    info = []

    if type(f_stats) != types.DictType:
        #PNFS is not that stable.  Different databases can hang, which
        # causes things like this stat() to hang.
        f_stats, (err, warn, info) = get_stat(f)

    if err or warn:
        errors_and_warnings(f, err, warn, info)
        return

    #If we are a supper user, reset the effective uid and gid.
    file_utils.acquire_lock_euid_egid()
    try:
        file_utils.set_euid_egid(f_stats[stat.ST_UID], f_stats[stat.ST_GID])
    except (KeyboardInterrupt, SystemExit):
        file_utils.release_lock_euid_egid()  # Release to avoid deadlock!
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except (OSError, IOError), msg:
        message = "Unable to set effective IDs (UID:%s, GID:%s) while  " \
                  "euid = %s  egid = %s  uid = %s  gid = %s [check()]:" \
                  " %s for %s\n" \
                  % (f_stats[stat.ST_UID], f_stats[stat.ST_GID],
                     os.geteuid(), os.getegid(), os.getuid(), os.getgid(),
                     str(msg), f)
        sys.stderr.write(message)
    except:
        file_utils.release_lock_euid_egid() # Release to avoid deadlock!
        message = "Unknown error setting effective IDs: %s" \
                  % (str(sys.exc_info()[1]),)
        sys.stderr.write(message)
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

    file_utils.release_lock_euid_egid()

    file_info = {"f_stats"       : f_stats}

    #There is no usual reason for the link count to be greater than 1.
    # There have been cases where a move was aborted early and two directory
    # entries were left pointing to one i-node where the i-node only had a
    # link count of 1 and not 2.  Since, there are legit reasons for multiple
    # hard links, don't consider it an error or warning.
    if f_stats[stat.ST_NLINK] > 1:
        info.append("link count(%d)" % f_stats[stat.ST_NLINK])

    # if f is a file, check its metadata
    if stat.S_ISREG(f_stats[stat.ST_MODE]):
        err_f, warn_f, info_f = check_file(f, file_info)
        errors_and_warnings(f, err + err_f, warn + warn_f, info + info_f)

    # if f is a directory, recursively check its files
    elif stat.S_ISDIR(f_stats[stat.ST_MODE]):

        if intf_of_scanfiles.threaded and is_new_database(f):
            #If we have the top directory of a new database, fork off a thread
            # for it.
            ts_check.append(ThreadWithResult(target = check_dir,
                                             args = (f, file_info)))
            ts_check[-1].start()
        else:
            err_d, warn_d, info_d = check_dir(f, file_info)
            if err_d or err or warn_d or warn or info_d or info:
                errors_and_warnings(f, err + err_d,
                                    warn + warn_d, info + info_d)

    # if f is a link, check that the target exists
    elif stat.S_ISLNK(f_stats[stat.ST_MODE]):
        err_l, warn_l, info_l = check_link(f, file_info)
        errors_and_warnings(f, err + err_l, warn + warn_l, info + info_l)

    else:
        err.append("unrecognized type")
        errors_and_warnings(f, err, warn, info)


def check_link(l, file_info):
    __pychecker__ = "unusednames=file_info"

    err = []
    warn = []
    info = []

    err, warn, info = check_parent(l)
    if err or warn:
        return err, warn, info

    if not os.path.exists(l):

        warn.append("missing the original of link")

    return err, warn, info


def check_dir(d, dir_info):

    d_stats = dir_info['f_stats']

    err = []
    warn = []
    info = []

    # skip volmap and .bad and .removed and migration directory
    #fname = os.path.basename(d)
    #if fname.lower().find("migration") != -1:
    #    return err, warn, info

    #If we are a supper user, reset the effective uid and gid.
    file_utils.acquire_lock_euid_egid()
    try:
        file_utils.set_euid_egid(d_stats[stat.ST_UID], d_stats[stat.ST_GID])
    except (KeyboardInterrupt, SystemExit):
        file_utils.release_lock_euid_egid() # Release to avoid deadlock!
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except OSError, msg:
        message = "Unable to set effective IDs (UID:%s, GID:%s) while  " \
                  "euid = %s  egid = %s  uid = %s  gid = %s [check_dir()]:" \
                  "%s for %s\n" \
                  % (d_stats[stat.ST_UID], d_stats[stat.ST_GID],
                     os.geteuid(), os.getegid(), os.getuid(), os.getgid(),
                     str(msg), d)
        sys.stderr.write(message)
    except:
        file_utils.release_lock_euid_egid() # Release to avoid deadlock!
        message = "Unknown error setting effective IDs: %s" \
                  % (str(sys.exc_info()[1]),)
        sys.stderr.write(message)
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    file_utils.release_lock_euid_egid()

    #Get the list of files.
    try:
        file_list = file_utils.listdir(d)
    except (OSError, IOError), msg:
        file_list = None #Error getting directory listing.

        if msg.args[0] == errno.ENOENT:
            err.append("does not exist")
        else:
            err.append("can not access directory")

    #file_list will be None on an error.  An empty list means the directory
    # is empty.
    if file_list != None:
        for i in range(0, len(file_list)):

            #This little check searches the files left in the list for
            # duplicate entries in the directory.
            if file_list[i] in file_list[i + 1:]:
                err.append("duplicate entry(%s)" % file_list[i])

            f = os.path.join(d, file_list[i])

            check(f)

    return err, warn, info

# check_vol(vol) -- check whole volume

def check_vol(vol):

    #We need to know if we should stop now.  Otherwise the main thread
    # will wait for child threads to finish.
    stop_threads_lock.acquire()
    if threads_stop:
        stop_threads_lock.release()
        thread.exit()
    stop_threads_lock.release()

    err = []
    warn = []
    info = []

    # Get list of files on tape:
    #   all_files=False -- get list of all physical files on tape
    #                      including packages but not constituent files.
    tape_ticket = infc.tape_list(vol,all_files=False,skip_unknown=True)
    if not e_errors.is_ok(tape_ticket):
        errors_and_warnings(vol, ['can not get tape_list info'], [], [])
        return
    volume_ticket, (err1, warn1, info1) = get_volumedb_info(vol)
    if err1 or warn1:
        errors_and_warnings(vol, ['can not get inquire_vol info'], [], [])
        return

    # Check if there are files with matching locations on this tape.
    tape_list = tape_ticket['tape_list']
    for i in range(len(tape_list)):
        if tape_list[i]['deleted'] in  ["yes", "unknown"]:
            continue
        for j in range(len(tape_list)):
            if j == i: # Skip the current file.
                continue
            if tape_list[j]['deleted'] in ["yes", "unknown"]:
                    continue
            if tape_list[i]['location_cookie'] == tape_list[j]['location_cookie']:
                if tape_list[i]['bfid'] < tape_list[j]['bfid']:
                    age = "another newer"
                elif tape_list[i]['bfid'] > tape_list[j]['bfid']:
                    #age = "another older"
                    continue
                else:
                    age = "" #Is this possible?
                #If we get here then we have multiple locations for the
                # same tape.
                ### Historical Note: This has been found to have happened
                ### while importing SDSS DLT data into Enstore due to a
                ### "get" bug.
                ### Note2: If the mover writes a tape but is unable to update
                ### the EOD cookie with the volume clerk, this can happen too.
                message = 'volume %s has %s duplicate location %s (%s, %s)' % \
                          (vol, age, tape_list[i]['location_cookie'],
                           tape_list[i]['bfid'], tape_list[j]['bfid'])
#                if tape_list[i]['deleted'] in  ["yes", "unknown"] or \
#                   tape_list[j]['deleted'] in ["yes", "unknown"]:
#                    #This is a possible situation:
#                    # 1) The file clerk assigns a new bfid.
#                    # 2) The volume clerk fails to update the EOD cookie.
#                    # 3) The error is propagated back to encp, resulting
#                    #    in a failure of the transfer.
#                    # 4) With the EOD cookie not updated the same position
#                    #    on tape is used.  Since the first file was
#                    #    deemed failed and marked deleted, we don't consider
#                    #    reaching this part of the scan a problem.
#                    continue
#                #elif volume_ticket['library'].find("shelf") != -1:
#                #    #If the volume is no longer available, we need to skip
#                #    # this check.
#                #    warn = [message,]
#                else:
#                    err = [message,]
                err = [message,]
                errors_and_warnings(vol, err, warn, info)
                break

    if intf_of_scanfiles.threaded:
        #Past evidence has shone that more than three
        # buckets/threads/coprocesses does not improve performance much,
        # if at all.
        THREADS = 3
        #Add one to make sure we don't miss any at the end.
        tapes_per_thread = (len(tape_list) / THREADS) + 1
        #Beginning and end of the first set of files on the tape.
        start = 0
        end = tapes_per_thread
        for i in range(THREADS):
            #Fork off a thread for each slice of the file list belong to
            # the volume.
            ts_check.append(ThreadWithResult(target = check_bit_files,
                                             args = (tape_list[start:end],
                                                     volume_ticket)))
            ts_check[-1].start()

            #Set the next loop to process the next section of the tape.
            start = end
            end = end + tapes_per_thread

        #Wait for the threads to finish.
        cleanup_threads()
    else:
        #Continue on with checking the bfids in one thread.
        check_bit_files(tape_list, volume_ticket)

#Intermediate function to handle scanning a list of files from check_vol(),
# with different lists scanned in different threads.
def check_bit_files(file_record_list, volume_record={}):
    global intf_of_scanfiles

    intf = intf_of_scanfiles
    for file_record in file_record_list:
        if file_record['deleted'] == "no" \
        or intf.with_deleted and file_record['deleted'] == "yes":
#           print file_record['bfid']," deleted ",file_record['deleted'] # DEBUG
            check_bit_file(file_record['bfid'],
                {'file_record' : file_record,'volume_record' : volume_record})

last_db_tried = ("", (-1, ""))
search_list = []

# check_bit_file(bfid) -- check file using bfid
#
# [1] get file record using bfid
# [2] get sfsid and sfs path
# [3] using sfs prefix and sfsid to find the real path
# [4] use real path to scan the file

def check_bit_file(bfid, bfid_info = None):
    global last_db_tried

    #We need to know if we should stop now.  Otherwise the main thread
    # will wait for child threads to finish.
    stop_threads_lock.acquire()
    if threads_stop:
        stop_threads_lock.release()
        thread.exit()
    stop_threads_lock.release()

    err = []
    warn = []
    info = []

    prefix = bfid

    if not bfid:
        err.append("no bfid given")
        errors_and_warnings(prefix, err, warn, info)
        return

    if bfid_info:
        file_record = bfid_info.get('file_record', None)
        volume_record = bfid_info.get('volume_record', None)
    else:
        file_record = None
        volume_record = None

    if not file_record:
        #If we don't have the file record already, go get it.
        file_record, (err1, warn1, info1) = get_filedb_info(bfid)
        if err1 or warn1:
            errors_and_warnings(prefix, err + err1, warn + warn1, info + info1)
            return
    else:
        #Verify the file record is correct.  (From volume tape_list().)
        (err1, warn1, info1) = verify_filedb_info(file_record)
        if err1 or warn1:
            errors_and_warnings(prefix, err + err1, warn + warn1, info + info1)
            return

    if not volume_record:
        #If we don't have the volume record already, go get it.
        volume_record, (err1, warn1, info1) = \
                       get_volumedb_info(file_record['external_label'])
        if err1 or warn1:
            errors_and_warnings(prefix, err + err1, warn + warn1, info + info1)
            return

    prefix = string.join([prefix, "...", file_record['external_label'],
                          file_record['location_cookie']], " ")

    #Determine if this file is a multiple copy.  Multiple copies made using
    # duplicate.py would set this True too.
    original_bfid = infc.find_original(bfid).get('original', None)
    if original_bfid != None and bfid != original_bfid:
        is_multiple_copy = True
    else:
        is_multiple_copy = False
        original_file_record = None
    ### This first method will be faster, but relies on an updated
    ### info_server.py.
    """
    #Determine if the file is an original copy.
    if original_bfid != None and bfid == original_bfid:
        is_primary_copy = True
    else:
        is_primary_copy = False
    """
    ### Use this method for now.
    all_copies = infc.find_all_copies(bfid).get('copies', None)
    if all_copies != None and len(all_copies) > 1:
        if is_multiple_copy:
            is_primary_copy = False
        else:
            is_primary_copy = True
    else:
        is_primary_copy = False

    #Determine if this file has been migrated/duplicated.  (Some early,
    # volumes were set to readonly instead of 'migrating', so include those
    # volumes.)  Just because the variable name is is_migrated_copy,
    # any cloned or duplicated files will also have this set true.
    response_dict = infc.find_migrated(bfid)
    if not e_errors.is_ok(response_dict):
        # We should simply be able to give the following warning if we
        # get this far.  However, to allow this scan to work on systems
        # that do not have the updated information server with
        # find_migrated(), we need to guess that if the system inhibit
        # says that the migration/duplication/cloning is done, that
        # we should just list the bfid as a migrated bfid.
        #      info.append("unable to determine migration status")
        if enstore_functions2.is_migrated_state(volume_record['system_inhibit'][1]):
            src_bfids = [bfid]
            dst_bfids = []
        elif enstore_functions2.is_migration_related_file_family(volume_record['file_family']):
            src_bfids = []
            dst_bfids = [bfid]
        else:
            src_bfids = [] #If nothing is found, an empty list is returned.
            dst_bfids = []
    else:
        src_bfids = response_dict['src_bfid']
        dst_bfids = response_dict['dst_bfid']

    if src_bfids and bfid in src_bfids:
        is_migrated_copy = True
    else:
        is_migrated_copy = False

    if dst_bfids and bfid in dst_bfids:
        is_migrated_to_copy = True
    else:
        is_migrated_to_copy = False

    #Include these information items if necessary.
    if is_multiple_copy and "is multiple copy" not in info:
        info.append("is multiple copy")
    elif is_primary_copy and "is primary copy" not in info:
        info.append("is primary copy")

    if is_migrated_copy and "is migrated copy" not in info:
        info.append("is migrated copy")
    elif is_migrated_to_copy and "is migrated to copy" not in info:
        info.append("is migrated to copy")

    #If the file is not active, add this information to the output.
    if file_record['deleted'] in ("yes", "unknown"):
        info_message = "deleted(%s)" % (file_record['deleted'],)
        if info_message not in info:
            info.append(info_message)

    # we can not simply skip deleted files
    #
    # for each deleted file, we have to make sure:
    #
    # [1] no sfsid, or
    # [2] no valid sfsid, or
    # [3] in reused sfsid case, the bfids are not the same
    if (is_migrated_copy or is_multiple_copy) and \
           file_record['deleted'] == 'yes':
        #The file is migrated.  The file was already deleted (and
        # --with-deleted was used) or the newly migrated to copy has been
        # scanned.  Either way there is no error.
        errors_and_warnings(prefix, err, warn, info)
        return
    if file_record['deleted'] in ["yes", "unknown"] and \
       not file_record['pnfsid']:
        #The file is deleted, no sfs id was recorded.  Not an error,
        # so move on to the next file.
        errors_and_warnings(prefix, err, warn, info)
        return
    if (is_migrated_copy or is_migrated_to_copy) and \
       file_record['deleted'] == "unknown":
        #The file is unknown, but yet recorded as being involved in migration.
        # This can not happen.  Unknown files can not be migrated and
        # there is a serious problem if a destination file is unknown.
        err.append("unknown file can not be involved in migration")
        errors_and_warnings(prefix, err, warn, info)
        return

    if is_multiple_copy:
        #We know this is a multiple copy, the storage file should point to
        # the orginal copy.  So lets use the original copies information
        # to find the path.
        original_file_record, (err1, warn1, info1) = get_filedb_info(original_bfid)
        if err1 or warn1:
            errors_and_warnings(prefix, err + err1, warn + warn1, info + info1)
            return

        use_bfid = original_bfid
        use_file_record = original_file_record
        use_pnfsid = original_file_record['pnfsid']
    else:
        use_bfid = bfid
        use_file_record = file_record
        use_pnfsid = file_record['pnfsid']

    #Loop over all found mount points.
    try:
        #Obtain the current path.  There are many ways to try and find the
        # file.  The original pathname is a good start, but there are a
        # lot of things to try before resorting to get_path().
        sfs_path = find_pnfs_file.find_id_path(use_pnfsid,
                                               use_bfid,
                                               file_record=use_file_record,
                                               use_info_server=True)
    except (OSError, IOError), msg:
        #For easier investigations, lets include the paths.
        sfs_path = getattr(msg, 'filename', "")
        if sfs_path == None:
            sfs_path = ""

        #The following list contains responses that we need to handle special.
        # These will accompany an errno of EEXIST.
        EXISTS_LIST = ["replaced with newer file",
                       "replaced with another file",
                       "found original of copy",
                       "reused pnfsid"]

        ### Note: msg.args[0] will be returned as ENOENT if a file is found
        ### to match the sfsid, but not the bfid.

        if (msg.errno == errno.ENOENT or \
            (msg.args[0] == errno.EEXIST and msg.args[1] in EXISTS_LIST)) and \
            (file_record['deleted'] in ["yes", "unknown"] or is_multiple_copy):
            # There is no error here.  Everything agrees the file is gone.
            # For multiple_copy files that should not have a record found
            # in the SFS, we don't want to flag an error either.
            pass

        ### Test for mulitple_copies/duplicates before plain migration.  This
        ### is because duplication also sets the is_migrated_copy or the
        ### is_migrated_to_copy values too.

        elif is_multiple_copy:
            #The multiple copy should not be findable, but in this
            # situation it was.
            err.append("multiple copy(%s)" % (msg.args[1],))
        elif is_primary_copy:
            #The primary copy should match the file, but in this situation
            # it did not.
            err.append("primary copy(%s)" % (msg.args[1],))

        ### Now plain source migration files can be handled.

        elif is_migrated_copy and file_record['deleted'] == "no":
            if msg.args[0] in [errno.ENOENT] or \
               (msg.args[0] == errno.EEXIST and msg.args[1] in EXISTS_LIST):
                #If the destination hasn't been scanned, this will be true.
                # If the destination has been scanned and we still get here,
                # does this check need to be furthur modified???
                warn.append("migrated copy not marked deleted")
            elif sfs_path and not namespace.is_id(sfs_path) \
                     and not (is_multiple_copy or is_primary_copy):
                #Catch the case where the migration source file is the active
                # file in the storage file system instead of the new copy.
                err.append("migration source copy is active in the SFS (%s)" \
                           % (sfs_path,))
            else:
                err.append("migration(%s)" % (msg.args[1],))
        elif is_migrated_copy and file_record['deleted'] == "yes":
            pass #Normal situation after scan, not an error.
        elif is_migrated_copy and file_record['deleted'] == "unknown":
            #Should never get here!
            err.append("failed (unknown) file found as migration source")

        ### Now the plain destination migration files can be handled.

        elif is_migrated_to_copy and file_record['deleted'] == "no":
            if msg.args[0] in [errno.ENOENT] or \
               (msg.args[0] == errno.EEXIST and msg.args[1] in EXISTS_LIST):
                #Test if the migration metadata in SFS is incorrectly swapped.
                try:
                    s = find_pnfs_file.find_id_path(file_record['pnfsid'],
                                                src_bfids[0],
                                                file_record=file_record,
                                                use_info_server=True)

                    #If find_id_path() succeeds here, we really have
                    # an error.  This test allows for a more accurate
                    # error message.
                    err.append("migration source copy is active in SFS (%s)" % (s,))
                except (OSError, IOError),msg2:
                    if msg2.args[0] in [errno.ENOENT]:
                        #Both source and destination failed to be found
                        # in the storage file system.
                        err.append("does not exist")
                    elif msg2.args[0] in [errno.EEXIST]:
                        original_bfid = infc.find_original(src_bfids[0]).get('original', None)
                        if original_bfid:
                            #We migrated a duplicte/multiple_copy.  While
                            # not a good thing it should not be considered
                            # an error.
                            pass
                        else:
                            err.append("migration(%s)" % (msg.args[1],))
                    else:
                        err.append("migration(%s)" % (msg.args[1],))
            else:
                err.append("migration(%s)" % (msg.args[1],))
        elif is_migrated_to_copy and file_record['deleted'] == "yes":
            if msg.args[0] == errno.ENOENT:
                #The file is marked deleted and not found in the storage
                # file system.
                pass #Normal situation
            elif msg.args[0] == errno.EEXIST and \
                 msg.args[1] in ["sfs entry exists"]:
                #The file is marked deleted and still exists in the storage
                # file system.
                err.append(msg.args[1])
            elif msg.args[0] == errno.EEXIST and \
                 msg.args[1] in EXISTS_LIST:
                #The file is marked deleted and a different file exists
                # in its place.
                pass #Normal situation
            else:
                err.append("migration(%s)" % (msg.args[1]))
        elif is_migrated_to_copy and file_record['deleted'] == "unknown":
            #Should never get here!
            err.append("failed (unknown) file found as migration destination")

        ###

        elif file_record['deleted'] in ['yes', 'unknown'] and \
                 msg.errno in [errno.EEXIST] and msg.args[1] in EXISTS_LIST:
            # The bfid is not active, and it is not active in the storage
            # file system.
            info.append(msg.args[1])
        elif msg.args[0] == errno.ENOENT and file_record['deleted'] == "no" \
                 and file_record['pnfs_name0'].find("/Migration/") != -1:
            #If a failed file sits on a migration destination volume,
            # we need to flag this so it can be flagged to be fixed.
            # This if statement is here to give a specific error message
            # instead of "No such file or directory".
            err.append("active failed migration destination file does not exist")
            info.append("deleted(no)")
        else:
            err.append(msg.args[1])

        if err or warn:
            errors_and_warnings(prefix, err, warn, info)
            return
    except (ValueError,), msg:
        err.append(str(msg))
        errors_and_warnings(prefix, err, warn, info)
        return

    #Stat the file.
    f_stats, (e2, w2, i2) = get_stat(sfs_path)
    err = err + e2
    warn = warn + w2
    info = info + i2
    if err or warn:
        errors_and_warnings(prefix, err, warn, info)
        return
    if stat.S_ISREG(f_stats[stat.ST_MODE]):
        file_info = {"f_stats"       : f_stats,
                     "layer1"        : bfid, # layer1_bfid,
                     "file_record"   : file_record,
                     "pnfsid"        : file_record['pnfsid']}
    else:
        #If this one-time file is no longer a file, then don't continue.
        # The check is necessary becuase there are some files that have
        # been replaced by directories of the same pathname.
        info.append("no longer a regular file")
        errors_and_warnings(prefix, err, warn, info)
        return

    #If we are a super user, reset the effective uid and gid.
    file_utils.acquire_lock_euid_egid()
    try:
        file_utils.set_euid_egid(f_stats[stat.ST_UID], f_stats[stat.ST_GID])
    except (KeyboardInterrupt, SystemExit):
        file_utils.release_lock_euid_egid() # Release to avoid deadlock!
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except OSError, msg:
        message = "Unable to set effective IDs (UID:%s, GID:%s) while  " \
                  "euid = %s  egid = %s  uid = %s  gid = %s [check_bit_file()]:" \
                  "%s for %s\n" \
                  % (f_stats[stat.ST_UID], f_stats[stat.ST_GID],
                     os.geteuid(), os.getegid(), os.getuid(), os.getgid(),
                     str(msg), sfs_path)
        sys.stderr.write(message)
    except:
        file_utils.release_lock_euid_egid() # Release to avoid deadlock!
        message = "Unknown error setting effective IDs: %s" \
                  % (str(sys.exc_info()[1]),)
        sys.stderr.write(message)
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

    file_utils.release_lock_euid_egid()

    file_info = {'f_stats'             : f_stats,
                 'layer1'              : bfid, #layer1_bfid,
                 'file_record'         : file_record,
                 'pnfsid'              : file_record['pnfsid'],
                 'is_multiple_copy'    : is_multiple_copy,
                 'is_primary_copy'     : is_primary_copy,
                 'volume_record'       : volume_record,
                 'is_migrated_copy'    : is_migrated_copy,
                 'is_migrated_to_copy' : is_migrated_to_copy,
                 }

    e1, w1, i1 = check_file(sfs_path, file_info)
    err = err + e1
    warn = warn + w1
    for item in i1:
        #We need to loop here to prevent duplicates from being inserted
        # into the list.
        if item not in info:
            info.append(item)
    errors_and_warnings(prefix + ' ' + sfs_path, err, warn, info)
    return

def check_file(f, file_info):

    f_stats = file_info['f_stats']
    bfid = file_info.get('layer1', None)
    filedb = file_info.get('file_record', None)
    #pnfs_id = file_info.get('pnfsid', None)
    is_multiple_copy = file_info.get('is_multiple_copy', None)
    is_primary_copy = file_info.get('is_primary_copy', None)
    volumedb = file_info.get('volume_record', None)
    is_migrated_copy = file_info.get('is_migrated_copy', None)
    is_migrated_to_copy = file_info.get('is_migrated_to_copy', None)

    err = []
    warn = []
    info = []

    fname = os.path.basename(f)

    #If the file is an (P)NFS or encp temporary file, give the error that
    # it still exists.
    if fname[:4] == ".nfs" or fname[-5:] == "_lock":
        err.append("found temporary file")
        if not f_stats:
            f_stats, (err_s, warn_s, info_s) = get_stat(f)
            err = err + err_s
            warn = warn + warn_s
            info = info + info_s
        return err, warn, info

    #Skip blacklisted files.
    if fname[:4] == '.bad':
        info.append("marked bad")
        #return err, warn, info #Non-lists skips any output.

    #Get the correct/current pnfs/chimera id for this file.
    sfs_id = get_sfsid(f)[0]

    #Get the info from layer 2.
    layer2, (err_2, warn_2, info_2) = get_layer_2(f)
    layer_2_from_name = layer2
    err = err + err_2
    warn = warn + warn_2
    info = info + info_2

    #If this file is not supposed to be forwarded to tape by dCache,
    # check the parent id and compare the file size from the os
    # and layer 2.
    if layer2.get('hsm', None) == "no":
        err_p, warn_p, info_p = check_parent(f)
        err = err + err_p
        warn = warn + warn_p
        info = info + info_p

        #Check for size.
        real_size = long(f_stats[stat.ST_SIZE])
        layer2_size = layer2.get('size', None)
        if layer2_size != None:
            layer2_size = long(layer2_size) #Don't cast a None.
            TWO_GIG_MINUS_ONE = 2147483648L - 1
            if real_size == 1L and layer2_size > TWO_GIG_MINUS_ONE:
                pass
            elif real_size == layer2_size:
                pass
            else:
                err.append("size(%s, %s)" % (layer2_size, real_size))
        elif layer2_size == None:
            warn.append("no layer 2 size")

        #Check to make sure that the CRC is present.
        if layer2.get('crc', None) == None:
            warn.append("no layer 2 crc")

        return err, warn, info

    #Even though we have the filename, we still need the .(access)() name
    # for some layer consistancy checks.
    afn = os.path.join(os.path.dirname(f), ".(access)(%s)" % (sfs_id,))

    #Get information from the layer 1 (if necessary).
    ## From a performance stand point, this sucks asking for the same
    ## information twice.  But it has been observed that asking for this
    ## information using the id and the name can give two different
    ## answers.
    layer_1_bfid_from_name, (err1, warn1, info1) = get_layer_1(f)
    layer_1_bfid_from_id, (err1a, warn1a, info1a) = get_layer_1(afn)
    err = union([err, err1, err1a])
    warn = union([warn, warn1, warn1a])
    info = union([info, info1, info1a])

    #Check to make sure that the storage file system is returning the same
    # information when getting layer 1 from the storage file system file id
    # and from the name.  There is one known case that this has happened.
    if layer_1_bfid_from_name != layer_1_bfid_from_id:
        message = "conflicting layer 1 values " \
                  "(id %s, name %s)" % \
                  (layer_1_bfid_from_id, layer_1_bfid_from_name)
        err.append(message)

    if not bfid:
        #We know that layer_1_bfid_from_name and layer_1_bfid_from_id must
        # be equal by this point.  Also, we were originally given a file
        # not bfid on the command line.
        bfid = layer_1_bfid_from_name

    layer_2_from_id = get_layer_2(afn)[0]

    #Check to make sure that the storage file system is returning the same
    # information when getting layer 2 from the storage file system file id
    # and from the name.  There is one known case that this has happened.
    if layer_2_from_name != layer_2_from_id:
        message = "conflicting layer 2 values " \
                  "(id %s, name %s)" % \
                  (layer_2_from_id, layer_2_from_name)
        err.append(message)

    #Get information from the layer 4.
    layer4, (err4, warn4, info4) = get_layer_4(f)
    layer_4_from_name = layer4
    layer_4_from_id, (err4a, warn4a, info4a) = get_layer_4(afn)
    err = union([err, err4, err4a])
    warn = union([warn, warn4, warn4a])
    info = union([info, info4, info4a])

    #Check to make sure that the storage file system is returning the same
    # information when getting layer 4 from the storage file system file id
    # and from the name.  There is one known case that this has happened.
    if layer_4_from_name != layer_4_from_id:
        message = "conflicting layer 4 values " \
                  "(id %s, name %s)" % \
                  (layer_4_from_id, layer_4_from_name)
        err.append(message)

    #If there are errors so far, report them now.
    if err or warn:
        return err, warn, info

    # Get file database information.
    if not filedb:
        filedb, (err_f, warn_f, info_f) = get_filedb_info(bfid)
        # Get file database errors.
        err = err + err_f
        warn = warn + warn_f
        info = info + info_f
        if err or warn:
            return err, warn, info

    # Get volume database information.
    if not volumedb and filedb:
        volumedb, (err_v, warn_v, info_v) = get_volumedb_info(
            filedb['external_label'])
        # Get file database errors.
        err = err + err_v
        warn = warn + warn_v
        info = info + info_v
        if err or warn:
            return err, warn, info

    #Look for missing storage filesystem information.
    try:
        #First we need to know if the file is deleted or not.
        if filedb:
            is_deleted = filedb.get("deleted", "unknown")
        else:
            is_deleted = "unknown"

        #Skip this check for situations where there are more copies of
        # this file.
        if not is_multiple_copy and not is_migrated_copy:
            #Handle the case where the file is active and the bfid conflicts.
            if bfid != layer4['bfid'] and is_deleted != "yes":
                err.append("bfid(%s, %s)" % (bfid, layer4['bfid']))
            #Handle the case where the file is deleted, but a matching
            # bfid was found in the storage filesystem layers.
            if bfid == layer4['bfid'] and is_deleted != "no":
                err.append("storage filesystem entry exists")

            #If the file is deleted and does not exist, there is no
            # reason to continue
            if is_deleted != "no":
                info.append("deleted(%s)" % (is_deleted,))
                return err, warn, info
    except (TypeError, ValueError, IndexError, AttributeError, KeyError):
    	age = time.time() - f_stats[stat.ST_MTIME]
        if age < ONE_DAY:
            warn.append('younger than 1 day (%d)' % (age))
        else:
            #If the size from stat(1) and layer 2 are both zero, then the
            # file really is zero length and the dCache did not forward
            # the file to tape/Enstore.
            if f_stats[stat.ST_SIZE] == 0L:
                if layer2.get('size', None) == 0L:
                    info.append("zero length dCache file not on tape")
                    return err, warn, info
                else:
                    err.append('missing file')
            else:
                if len(bfid) < 8:
                    err.append('missing layer 1')

                if not layer4.has_key('bfid'):
                    err.append('missing layer 4')
                    return err, warn, info

                if layer2.get('pools', None):
                    info.append("pools(%s)" % (layer2['pools'],))

        #Use this information to determine if the filename
        # did correspond to a valid file.
        for fname in [f, get_enstore_pnfs_path(f),
                      get_dcache_pnfs_path(f)]:
            ffbp = infc.find_file_by_path(fname)
            if e_errors.is_ok(ffbp):
                if ffbp.has_key('file_list'):
                    file_list = ffbp['file_list']
                else:
                    file_list = [ffbp]

                #It is possible to get a list of matching records by name.
                # Loop through them looking for a match of id.
                for file_record in file_list:
                    if file_record['pnfsid'] in ["", None, "None"]:
                        #err.append('no pnfs id in db')
                        break

                    try:
                        sfs = namespace.StorageFS(f)
                        cur_sfsid = sfs.get_id(f) #sfs of current searched file
                        unused = sfs.get_path(file_record['pnfsid'],
                                              os.path.dirname(f))

                        #Deal with multiple possible matches.
                        if len(unused) != 1:
                            err.append("to many matches %s" % (unused,))
                            return err, warn, info

                        rm_sfs = False

                        #Set the current record to be the one we use.
                        ffbp = file_record
                        break
                    except (OSError, IOError), msg:
                        if msg.args[0] == errno.ENOENT:
                            rm_sfs = True
                        else:
                            rm_sfs = None  #Unknown
                    except (ValueError,), msg:
                        rm_sfs = None #Unknown
                else:
                    #No matching record was found in the file DB.
                    err.append("no storage fs id matches for")
                    return err, warn, info

                if ffbp['deleted'] == "yes":
                    marked_deleted = True
                elif ffbp['deleted'] == "no":
                    marked_deleted = False
                else:
                    marked_deleted = None

                if marked_deleted and rm_sfs:
                    description = "deleted file"
                elif marked_deleted != None and not marked_deleted \
                         and rm_sfs != None and not rm_sfs:
                    description = "active file"
                else:
                    description = "file"

                #This block of code will modifiy the description
                # to confirm that the "file" found in the enstore
                # db is an older file.  Give a 1 hour buffer.
                try:
                    use_bfid = ffbp['bfid'].split("_")[0]
                    rmatch = re.compile("[0-9]*$")
                    match_result = rmatch.search(use_bfid)
                    found_time = long(match_result.group(0)[:-5])
                    found_time_string = time.ctime(found_time)
                except:
                    found_time = None
                    found_time_string = ""
                if found_time and \
                       found_time + 600 < f_stats[stat.ST_MTIME]:
                    description = "older " + description

                #Include this additional information in the error.
                info.append("found %s with same name (%s)" % \
                            (description, ffbp['bfid'],))
                info.append("this file (%s, %s)  found file (%s, %s)" %
                            (time.ctime(f_stats[stat.ST_MTIME]),
                             ffbp['pnfsid'],
                             found_time_string,
                             cur_sfsid))

                break

    if err or warn:
        return err, warn, info


    # volume label
    try:
        if not is_multiple_copy and not is_migrated_copy:
            if layer4['volume'] != filedb['external_label']:
                err.append('label(%s, %s)' % (layer4['volume'],
                                              filedb['external_label']))
    except (TypeError, ValueError, IndexError, AttributeError):
        err.append('no or corrupted external_label')

    # location cookie
    try:
        if not is_multiple_copy and not is_migrated_copy:
            #The location cookie is split into three sections.  All but
            # the eariest files use only the last of these three sections.
            # Thus, this check makes sure that (1) the length of both
            # original strings are the same and (2) only the last section
            # matches exactly.
            p_lc = string.split(layer4['location_cookie'], '_')[2]
            f_lc = string.split(filedb['location_cookie'], '_')[2]
            if p_lc != f_lc or \
                   len(layer4['location_cookie']) != \
                   len(filedb['location_cookie']):
                err.append('location_cookie(%s, %s)' %
                           (layer4['location_cookie'],
                            filedb['location_cookie']))
    except (TypeError, ValueError, IndexError, AttributeError):
        #Before writting this off as an error, first determine if this
        # is a disk mover location cookie.
        p_lc = layer4['location_cookie']
        f_lc = filedb['location_cookie']
        if p_lc == f_lc and enstore_functions3.is_location_cookie_disk(f_lc):
            #This is a valid disk mover location cookie.
            pass
        else:
            err.append('no or corrupted location_cookie')

    # size
    try:
        real_size = f_stats[stat.ST_SIZE]

        if long(layer4['size']) != long(filedb['size']):
            err.append('size(%d, %d, %d)' % (long(layer4['size']),
                                             long(real_size),
                                             long(filedb['size'])))
        elif real_size != 1 and long(real_size) != long(layer4['size']):
            err.append('size(%d, %d, %d)' % (long(layer4['size']),
                                             long(real_size),
                                             long(filedb['size'])))

        if layer2 and layer2.get('size', None) == None:
            warn.append("no layer 2 size")
        elif layer2.get('size', None) != None:
            if long(layer2['size']) != long(filedb['size']):
                # Report if Enstore DB and the dCache size in the storage
                # file system  layer 2 are not the same.
                err.append('dcache_size(%s, %s)' % (layer2['size'],
                                                    filedb['size']))
    except (TypeError, ValueError, IndexError, AttributeError):
        err.append('no or corrupted size')

    # file_family and library
    try:
        file_family = volumedb['file_family']
        library =  volumedb['library']

        # File Family check.  Take care of MIGRATION and duplication, too.
        if layer4['file_family'] != file_family and \
               layer4['file_family'] + '-MIGRATION' != file_family and \
               not file_family.startswith(layer4['file_family'] + "_copy_"):
            info.append('file_family(%s, %s)' % (layer4['file_family'],
                                                 file_family))
        # Library Manager check.
        if library not in lm:
            #Skip reporting on shelf libraries.
            if library.find("shelf") == -1:
                err.append('no such library (%s)' % (library))
    except (TypeError, ValueError, IndexError, AttributeError):
        err.append('no or corrupted file_family')

    # drive
    try:
        if not is_multiple_copy and not is_migrated_copy:
            # some do not have this field
            if filedb.has_key('drive') and layer4.has_key('drive'):
                if layer4['drive'] != filedb['drive']:
                    if layer4['drive'] != 'imported' \
                           and layer4['drive'] != "missing" \
                           and filedb['drive'] != 'unknown:unknown':
                        err.append('drive(%s, %s)' % (layer4['drive'],
                                                  filedb['drive']))
            elif not layer4.has_key('drive'):
                warn.append("missing layer 4 drive")  #not fatal
    except (TypeError, ValueError, IndexError, AttributeError):
        err.append('no or corrupted drive')

    # CRC
    try:
        if layer4.get('crc', "") == "":  # some do not have this field
            warn.append("missing layer 4 crc")  #not fatal
        else:
            if long(layer4['crc']) != long(filedb['complete_crc']):
                # Report if Enstore DB and the Enstore CRC in the storage
                # file system layer 4 are not the same.
                err.append('crc(%s, %s)' % (layer4['crc'],
                                            filedb['complete_crc']))
        # Comparing the Enstore and dCache CRCs must be skipped if the
        # volume in question is a null volume.
        if layer2 and layer2.get('size', None) == None:
            warn.append("no layer 2 crc")
        elif volumedb['media_type'] != "null" and \
               layer2.get('crc', None) != None: # some do not have this field
            crc_1_seeded = checksum.convert_0_adler32_to_1_adler32(
                filedb['complete_crc'], filedb['size'])
            #We need to compare both the unconverted and converted CRC.
            # There may come a time when we have a mixed 0 and 1 seeded
            # environment.
            if long(layer2['crc']) != long(crc_1_seeded) and \
                   long(layer2['crc']) != long(filedb['complete_crc']):
                # Report if Enstore DB and the dCache CRC in the storage
                # file system layer 2 are not the same.
                err.append('dcache_crc(%s, %s)' % (layer2['crc'],
                                                   crc_1_seeded))
    except (TypeError, ValueError, IndexError, AttributeError):
        err.append('no or corrupted CRC')

    # path
    try:
        layer4_name = get_enstore_pnfs_path(layer4.get('original_name', "NO-LAYER4_NAME"))
        current_name = get_enstore_pnfs_path(f)
        #filedb_name = get_enstore_pnfs_path(filedb.get('pnfs_name0', "NO-FILEDB-NAME"))
        #For original copies, if the layer 4 and file DB original paths do
        # not match, give an error.
        #
        #Multiple copies can not be subject to this test.  A moved/renamed
        # original copy will modify the layer 4 value for the multiple copy,
        # too.
        if not is_multiple_copy and \
           layer4['original_name'] != filedb['pnfs_name0']: #layer4 vs filedb
            #print layer 4, current name, file database.  ERROR
            err.append("filename(%s, %s, %s)" %
                       (layer4['original_name'], f, filedb['pnfs_name0']))
        elif is_access_name(f):
            #Skip the renamed and moved tests if the filename of
            # the type ".(access)()".
            pass
        elif f != layer4['original_name']: # current pathname vs. layer 4
            if os.path.basename(current_name) != os.path.basename(layer4_name):
                #print current name, then original name.  INFORMATIONAL
                info.append('renamed(%s, %s)' % (f, layer4['original_name']))
            if os.path.dirname(current_name) != os.path.dirname(layer4_name):
                #print current name, then original name.  INFORMATIONAL
                info.append('moved(%s, %s)' % (f, layer4['original_name']))
    except (TypeError, ValueError, IndexError, AttributeError):
        err.append('no or corrupted path')

    # sfsid
    try:
        use_layer4_sfsid = layer4.get('pnfsid', "NO-LAYER4-SFSID")
        use_filedb_sfsid = filedb.get('pnfsid', "NO-FILEDB-SFSID")
        if sfs_id != use_layer4_sfsid  or sfs_id != use_filedb_sfsid:
            err.append('sfsid(%s, %s, %s)' % (use_layer4_sfsid, sfs_id,
                                               use_filedb_sfsid))
    except (TypeError, ValueError, IndexError, AttributeError):
        err.append('no or corrupted storage file system id')

    # parent id
    try:
        parent_id = get_parent_id(f)[0]
        parent_dir_id = get_parent_dir_id(f)[0]

        #Actually perform this check that is common to all types of files.
        if parent_id != parent_dir_id:
            if parent_id != None and parent_dir_id != None:
                alt_path = os.path.join(os.path.dirname(f),
                                        ".(access)(%s)" % parent_id, fname)
                try:
                    alt_stats = get_stat(alt_path)

                    if f_stats != alt_stats:
                        err.append("parent_id(%s, %s)" % (parent_id,
                                                          parent_dir_id))
                except OSError, msg:
                    if msg.args[0] not in [errno.ENOENT]:
                        #It is quite possible that a user can create
                        # multiple hardlinks to a file.  It is even
                        # possible that they could remove the original
                        # file.
                        err.append("parent_id(%s, %s)" % (parent_id,
                                                          parent_dir_id))
    except (TypeError, ValueError, IndexError, AttributeError):
        err.append('no or corrupted parent id')

    # deleted
    try:
        if filedb['deleted'] != 'no':
            err.append('deleted(%s)' % (filedb['deleted']))
    except (TypeError, ValueError, IndexError, AttributeError):
        err.append('no deleted field')

    #Include these information items if necessary.
    if is_multiple_copy and "is multiple copy" not in info:
        info.append("is multiple copy")
    elif is_primary_copy and "is primary copy" not in info:
        info.append("is primary copy")

    if is_migrated_copy and "is migrated copy" not in info:
        info.append("is migrated copy")
    elif is_migrated_to_copy and "is migrated to copy" not in info:
        info.append("is migrated to copy")

    return err, warn, info


def check_parent(f):

    err = []
    warn = []
    info = []

    parent_id, unused = get_parent_id(f)
    parent_dir_id, unused = get_parent_dir_id(f)

    #Actually perform this check that is common to all types of files.
    if parent_id != None and parent_dir_id != None and \
           parent_id != parent_dir_id:
        err.append("parent_id(%s, %s)" % (parent_id, parent_dir_id))

    return err, warn, info

###############################################################################

def start_check(line):
    global sfs

    line = os.path.abspath(line.strip())

    #Sanity check incase of user error specifying a non-storage file
    # system path.
    if not namespace.is_storage_path(line, check_name_only = 1):
        error(line+' ... not a storage filesystem file or directory')
        return

    """
    import profile
    import pstats
    profile.run("check(line)", "/tmp/scanfiles_profile")
    p = pstats.Stats("/tmp/scanfiles_profile")
    p.sort_stats('cumulative').print_stats(100)
    """

    check(line)

    #Wait for the threads to finish.
    cleanup_threads()

###############################################################################

class ScanfilesInterface(option.Interface):
    def __init__(self, args=sys.argv, user_mode=0):

        self.infile = None
        self.bfid = 0
        self.vol = 0
        self.file_threads = 3
        self.directory_threads = 1
        self.profile = 0
        self.old_path = []
        self.new_path = []
        self.threaded = 0
        self.with_deleted = False

        option.Interface.__init__(self, args=args, user_mode=user_mode)

    def valid_dictionaries(self):
        return (self.help_options, self.scanfile_options)

    #  define our specific parameters
    parameters = ["[TARGET_PATHS | BFIDS | VOLUMES]"]

    scanfile_options = {
        # --bfid is considered obsolete:
        option.BFID:{option.HELP_STRING:"treat input as bfids",
                option.VALUE_USAGE:option.IGNORED,
                option.DEFAULT_VALUE:option.DEFAULT,
                option.DEFAULT_TYPE:option.INTEGER,
                option.USER_LEVEL:option.HIDDEN},
        option.FILE_THREADS:{option.HELP_STRING:"number of processing threads",
                option.VALUE_USAGE:option.REQUIRED,
                option.VALUE_TYPE:option.INTEGER,
                option.USER_LEVEL:option.HIDDEN,},
        option.INFILE:{option.HELP_STRING:"read list of targets to scan from file",
                option.VALUE_USAGE:option.REQUIRED,
                option.VALUE_TYPE:option.STRING,
                option.USER_LEVEL:option.USER,},
        option.PROFILE:{option.HELP_STRING:"display profile info on exit",
                option.VALUE_USAGE:option.IGNORED,
                option.USER_LEVEL:option.ADMIN,},
        option.THREADED:{option.HELP_STRING:"use multiple threads"
                " (will interlace the output)",
                option.DEFAULT_TYPE:option.INTEGER,
                option.DEFAULT_NAME:"threaded",
                option.DEFAULT_VALUE:1,
                option.USER_LEVEL:option.USER,},
        option.WITH_DELETED:{option.HELP_STRING:"Include deleted files in volume scan",
                option.VALUE_USAGE:option.IGNORED,
                option.VALUE_TYPE:option.INTEGER,
                option.USER_LEVEL:option.USER,},
        # --vol is considered obsolete:
        option.VOL:{option.HELP_STRING:"treat input as volumes",
                option.VALUE_USAGE:option.IGNORED,
                option.DEFAULT_VALUE:option.DEFAULT,
                option.DEFAULT_TYPE:option.INTEGER,
                option.USER_LEVEL:option.HIDDEN},
        }

    def parse_options(self):
        # normal parsing of options
        option.Interface.parse_options(self)

        #Process these at the beginning.
        if hasattr(self, "help") and self.help:
            self.print_help()
        if hasattr(self, "usage") and self.usage:
            self.print_usage()


def handle_signal(sig, frame):
    __pychecker__ = "unusednames=sig,frame"
    global threads_stop

    #Tell other threads to stop now.
    stop_threads_lock.acquire()
    threads_stop = True
    stop_threads_lock.release()

    sys.exit(1)

def main(intf_of_scanfiles, file_object, file_list):
    # intf_of_scanfiles has been used before.  Probably will be used again.
    # This quiets pychecker for the time being.
    __pychecker__ = "unusednames=intf_of_scanfiles"

    try:
        #When the entire list of files/directories is listed on the command
        # line we need to loop over them.
        if file_list:
            for line in file_list:
                if line[:2] != '--':
                    if bfid_util.is_bfid(line):
                        check_bit_file(line)
                    elif enstore_functions3.is_volume(line):
                        check_vol(line)
                    else:
                        start_check(line)

        #When the list of files/directories is of an unknown size from a file
        # object; read the filenames in one at a time for resource efficiency.
        elif file_object:
            line = file_object.readline()
            while line:
                line = line.split(" ... ")[0].strip()
                if bfid_util.is_bfid(line):
                    check_bit_file(line)
                elif enstore_functions3.is_volume(line):
                    check_vol(line)
                else:
                    start_check(line)
                line = file_object.readline()

    except (KeyboardInterrupt, SystemExit):
        #If the user does Control-C don't traceback.
        pass


def do_work(intf):
    global infc
    global lm
    global intf_of_scanfiles

    #Hack for check() to access intf_of_scanfiles.
    intf_of_scanfiles = intf

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    if intf_of_scanfiles.infile:
        try:
            file_object = open(intf_of_scanfiles.infile)
        except (OSError, IOError), msg:
            sys.stderr.write("%s\n" % (str(msg),))
            sys.exit(1)
        file_list = None
    elif len(intf_of_scanfiles.args) == 0:
        file_object = sys.stdin
        file_list = None
    else:
        file_object = None
        # file_list = sys.argv[1:]
        file_list = intf_of_scanfiles.args

    csc = configuration_client.ConfigurationClient(
        (intf_of_scanfiles.config_host,
         intf_of_scanfiles.config_port))

    #Get the list of library managers.
    #    This old way had the disadvantage that if you wanted to run
    #      offline database scans you needed to have fully configured
    #      libraries in the offline config file instead of just stubs.
    #    lm = csc.get_library_managers().keys()
    lm = []
    config_dict = csc.dump_and_save()
    for item in config_dict.keys():
        if item[-16:] == ".library_manager":
            lm.append(item[:-16])

    flags = enstore_constants.NO_LOG | enstore_constants.NO_ALARM
    infc = info_client.infoClient(csc, flags = flags)

    main(intf_of_scanfiles, file_object, file_list)


if __name__ == "__main__":

    intf_of_scanfiles = ScanfilesInterface(sys.argv, 0) # zero means admin

    if intf_of_scanfiles.profile:
        import profile
        import pstats
        profile.run("do_work(intf_of_scanfiles)",
                    "/tmp/scanfiles_profile")
        p = pstats.Stats("/tmp/scanfiles_profile")
        p.sort_stats('cumulative').print_stats(100)
    else:
        do_work(intf_of_scanfiles)
