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
import info_client
import configuration_client
import option
import pnfs
import volume_family
import e_errors
import Trace
import charset
import enstore_constants
import checksum
import find_pnfs_file
import enstore_functions2

#os.access = e_access   #Hack for effective ids instead of real ids.

class ThreadWithResult(threading.Thread):
    #def __init__(self, *pargs, **kwargs):
    #    threading.Thread.__init__(self, *pargs, **kwargs)
    #    self.result = None

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
        except KeyError:
            exc, msg, tb = sys.exc_info()
            import traceback
            traceback.print_tb(tb)
            print str(exc), str(msg)

        # and now the thread exits

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

#The os.access() and the access(2) C library routine use the real id when
# testing for access.  This function does the same thing but for the
# effective ID.
def e_access(path, mode):
    
    #Test for existance.
    try:
        file_stats = os.stat(path)
        #stat_mode = file_stats[stat.ST_MODE]
    except OSError:
        return 0

    return check_permissions(file_stats, mode)

def check_permissions(file_stats, mode):

    stat_mode = file_stats[stat.ST_MODE]

    #Make sure a valid mode was passed in.
    if mode & (os.F_OK | os.R_OK | os.W_OK | os.X_OK) != mode:
        return 0

    # Need to check for each type of access permission.

    if mode == os.F_OK:
        # In order to get this far, the file must exist.
        return 1

    if mode & os.R_OK:  #Check for read permissions.
        #If the user is user root.
        if os.geteuid() == 0:
            #return 1
            pass
        #Anyone can read this file.
        elif (stat_mode & stat.S_IROTH):
            #return 1
            pass
        #This is the files owner.
        elif (stat_mode & stat.S_IRUSR) and \
           file_stats[stat.ST_UID] == os.geteuid():
            #return 1
            pass
        #The user has group access.
        elif (stat_mode & stat.S_IRGRP) and \
           (file_stats[stat.ST_GID] == os.geteuid() or
            file_stats[stat.ST_GID] in os.getgroups()):
            #return 1
            pass
        else:
            return 0

    if mode & os.W_OK:  #Check for write permissions.
        #If the user is user root.
        if os.geteuid() == 0:
            #return 1
            pass
        #Anyone can write this file.
        elif (stat_mode & stat.S_IWOTH):
            #return 1
            pass
        #This is the files owner.
        elif (stat_mode & stat.S_IWUSR) and \
           file_stats[stat.ST_UID] == os.geteuid():
            #return 1
            pass
        #The user has group access.
        elif (stat_mode & stat.S_IWGRP) and \
           (file_stats[stat.ST_GID] == os.geteuid() or
            file_stats[stat.ST_GID] in os.getgroups()):
            #return 1
            pass
        else:
            return 0
    
    if mode & os.X_OK:  #Check for execute permissions.
        #If the user is user root.
        if os.geteuid() == 0:
            #return 1
            pass
        #Anyone can execute this file.
        elif (stat_mode & stat.S_IXOTH):
            #return 1
            pass
        #This is the files owner.
        elif (stat_mode & stat.S_IXUSR) and \
           file_stats[stat.ST_UID] == os.geteuid():
            #return 1
            pass
        #The user has group access.
        elif (stat_mode & stat.S_IXGRP) and \
           (file_stats[stat.ST_GID] == os.geteuid() or
            file_stats[stat.ST_GID] in os.getgroups()):
            #return 1
            pass
        else:
            return 0

    return 1

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
        #This should never happen.
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

def parent_file(f, pnfsid = None):
    pn, fn = os.path.split(f)
    if pnfsid:
        return os.path.join(pn, ".(parent)(%s)" % (pnfsid))
    if is_access_name(f):
        pnfsid = fn[10:-1]
        return os.path.join(pn, ".(parent)(%s)" % (pnfsid))
    else:
        fname = id_file(f)
        f = open(fname)
        pnfsid = f.readline()
        f.close()
        return os.path.join(pn, ".(parent)(%s)" % (pnfsid))

def access_file(dn, pnfsid):
    return os.path.join(dn, ".(access)(%s)" % (pnfsid))

def database_file(directory):
    return os.path.join(directory, ".(get)(database)")

###############################################################################

def get_database(f):
    #err = []
    #warn = []
    #info = []
    
    db_a_dirpath = pnfs.get_directory_name(f)
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
    RETRY_COUNT = 2
    
    i = 0
    while i < RETRY_COUNT:
        # get info from layer
        try:
            fl = open(layer_filename)
            layer_info = fl.readlines()
            fl.close()
            break
        except (OSError, IOError), detail:
            #Increment the retry count before it is needed to determine if
            # we should sleep or not sleep.
            i = i + 1
            
            if detail.args[0] in [errno.EACCES, errno.EPERM] and os.getuid() == 0:
                #If we get here and the real id is user root, we need to reset
                # the effective user id back to that of root ...
                os.seteuid(0)
                os.setegid(0)
            elif i < RETRY_COUNT:
                #If the problem wasn't permissions, lets give the system a
                # moment to catch up.
                #Skip the sleep if we are not going to try again.
                ##time.sleep(0.1)

                ##It is known that stat() can return and incorrect ENOENT
                ## if pnfs is really loaded.  Is this true for open() or
                ## readline()?  Skipping the time.sleep() makes the scan
                ## much faster.
                raise detail
    else:
        raise detail

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

def get_layers(f):
    bfid, (err, warn, info) = get_layer_1(f)
    layer4, (err1, warn1, info1) = get_layer_4(f)

    return (bfid, layer4), (err + err1, warn + warn1, info + info1)

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
        if fr.get('deleted', None) == "no":
            # Look for missing file database information.
            filedb_pnfs_name0 = fr.get('pnfs_name0', "")
            if filedb_pnfs_name0 in ["", None, "None"]:
                err.append('no filename in db')
            filedb_pnfsid = fr.get('pnfsid', "")
            if filedb_pnfsid in ["", None, "None"]:
                err.append('no pnfs id in db')
            elif not pnfs.is_pnfsid(filedb_pnfsid):
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
            f_stats = os.lstat(f)

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
            dir_list = os.listdir(directory)
            if filename in dir_list:
                #We have this special error situation.
                err.append("invalid directory entry")
                return None, (err, warn, info)
        except OSError:
            pass

        err.append("does not exist")
        return None, (err, warn, info)

    if msg.errno == errno.EACCES or msg.errno == errno.EPERM:
        if os.getuid() == 0: #If we started as user root...
            try:
                os.seteuid(0)  #...reset the effective uid and gid.
                os.setegid(0)

                f_stats = os.lstat(f)  #Redo the stat.
                return f_stats, (err, warn, info)
            except OSError, msg:
                pass

        #If we get here we still could not stat the file.
        err.append("permission error")
        return None, (err, warn, info)

    err.append(os.strerror(msg.errno))
    return None, (err, warn, info)

    #return f_stats, (err, warn, info)

def get_pnfsid(f):

    err = []
    warn = []
    info = []

    if is_access_name(f):
        pnfsid = os.path.basename(f)[10:-1]
        return pnfsid, (err, warn, info)
    
    #Get the id of the file or directory.
    try:
        fname = id_file(f)
        f = open(fname)
        pnfs_id = f.readline().strip()
        f.close()
    except(OSError, IOError), detail:
        pnfs_id = None
        if not detail.errno == errno.ENOENT or not os.path.ismount(f):
            err.append("unable to obtain pnfs id")

    return pnfs_id, (err, warn, info)

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
    pnfs_id, (err2, warn2, info2) = get_pnfsid(f)

    return (pnfs_id, parent_id, parent_dir_id), \
           (err + err1 + err2, warn + warn1 + warn2, info + info1 + info2)

###############################################################################

#Global cache.
db_pnfsid_cache = {}

def parse_mtab():
    global db_pnfsid_cache
    
    #Clear this out to remove stale entries.
    db_pnfsid_cache = {}

    for mtab_file in ["/etc/mtab", "/etc/mnttab"]:
        try:
            fp = open(mtab_file, "r")
            mtab_data = fp.readlines()
            fp.close()
            break
        except OSError, msg:
            if msg.args[0] in [errno.ENOENT]:
                continue
            else:
                raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    else:
        #Should this raise an error?
        mtab_data = []
        
    for line in mtab_data:
        #The 2nd and 3rd items in the list are important to us here.
        data = line[:-1].split()
        mp = data[1]
        fs_type = data[2]

        #If the filesystem is not an NFS filesystem, skip it.
        if fs_type != "nfs":
            continue

        try:
            dataname = os.path.join(mp, ".(get)(database)")
            db_fp = open(dataname, "r")
            db_data = db_fp.readline().strip()
            db_fp.close()
        except IOError:
            continue

        db_datas = db_data.split(":")
        #db_datas[0] is the database name
        #db_datas[1] is the database id
        #db_datas[2] is the database (???)
        #db_datas[3] is the database enabled or disabled status
        #db_datas[4] is the database (???)

        #If the database's id is not in the cache, add it along with the
        # mount point that goes with it.
        db_pnfsid = int(db_datas[1])
        if db_data not in db_pnfsid_cache.keys():
            db_pnfsid_cache[db_data] = (db_pnfsid, mp)

def process_mtab():
    global db_pnfsid_cache
    global search_list
    
    if not db_pnfsid_cache:
        #Sets global db_pnfsid_cache.
        parse_mtab()
        
    p = pnfs.Pnfs()
    for database_info, (db_num, mp) in db_pnfsid_cache.items():
        if db_num == 0 or os.path.basename(mp) == "fs":
            #For /pnfs/fs we need to find all of the /pnfs/fs/usr/* dirs.
            use_path = os.path.join(mp, "usr")
            for dname in os.listdir(use_path):
                tmp_name = os.path.join(use_path, dname)
                if not os.path.isdir(tmp_name):
                    continue
                tmp_db_info = p.get_database(tmp_name).strip()
                if tmp_db_info in db_pnfsid_cache.keys():
                    continue
                
                tmp_db = int(tmp_db_info.split(":")[1])
                db_pnfsid_cache[tmp_db_info] = (tmp_db, tmp_name)

    sort_mtab()

def __db_cmp(x, y):
    is_x_fs_usr = x[1][1].find("/fs/usr/") > 0
    is_y_fs_usr = y[1][1].find("/fs/usr/") > 0

    is_x_fs = x[1][0] == 0
    is_y_fs = y[1][0] == 0

    #Always put /pnfs/fs last.
    if is_x_fs and not is_y_fs:
        return 1
    elif not is_x_fs and is_y_fs:
        return -1

    #Always put /pnfs/xyz first.
    elif is_x_fs_usr and not is_y_fs_usr:
        return 1
    elif not is_x_fs_usr and is_y_fs_usr:
        return -1

    #The are the same type of path.  Sort by db number.
    if x[1][0] < y[1][0]:
        return 1
    elif x[1][0] > y[1][0]:
        return -1

    return 0

def sort_mtab():
    global db_pnfsid_cache
    global search_list

    search_list = db_pnfsid_cache.items()
    #By sorting and reversing, we can leave db number 0 (/pnfs/fs) in
    # the list and it will be sorted to the end of the list.
    search_list.sort(lambda x, y: __db_cmp(x, y))

    #import pprint
    #pprint.pprint(search_list)
    #sys.exit(1)

def add_mtab(db_info, db_num, db_mp):
    global db_pnfsid_cache

    if db_info not in db_pnfsid_cache.keys():
        db_pnfsid_cache[db_info] = (db_num, db_mp)
        sort_mtab()

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
    if os.getuid() == 0:
        if os.geteuid() != f_stats[stat.ST_UID]:
            if os.geteuid() != 0:
                #If the currect effective ids are not currently root,
                # we need to set them back before (re)setting them.
                try:
                    os.setegid(0)
                    os.seteuid(0)
                except OSError:
                    pass

            #Set the uid and gid to match that of the file's owner.
            try:
                os.setegid(f_stats[stat.ST_GID])
                os.seteuid(f_stats[stat.ST_UID])
            except OSError:
                pass
    
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

        if is_new_database(f):
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
    fname = os.path.basename(d)
    if fname.lower().find("migration") != -1: # or \
        #fname == 'volmap' or \
        #fname[:3] == '.B_' or \
        #   fname[:3] == '.A_' or \
        #fname[:8] == '.removed':
        return err, warn, info
   
    if not check_permissions(d_stats, os.R_OK | os.X_OK) and \
       os.getuid() == 0 and os.geteuid() != 0:
        #We might need to switch back to root, since the user might not
        # have given themselves eXecute permissions on the directory.
        os.seteuid(0)
        os.setegid(0)
    if check_permissions(d_stats, os.R_OK | os.X_OK):
        err, warn, info = check_parent(d)
        if err or warn:
            return err, warn, info

        #Get the list of files.
        try:
            file_list = os.listdir(d)
        except (OSError, IOError):
            #If we call check_permissions() above, how can we possibly,
            # get here?  Clearly, it is possible though...

            err.append("can not access directory")
            file_list = []
            

        for i in range(0, len(file_list)):

            #This little check searches the files left in the list for
            # duplicate entries in the directory.
            if file_list[i] in file_list[i + 1:]:
                err.append("duplicate entry(%s)" % file_list[i])
                
            f = os.path.join(d, file_list[i])

            check(f)

    else:
        err.append("can not access directory")
    
    return err, warn, info

# check_vol(vol) -- check whole volume

def check_vol(vol):
    err = []
    warn = []
    info = []
    
    tape_ticket = infc.tape_list(vol)
    if not e_errors.is_ok(tape_ticket):
        errors_and_warnings(vol, ['can not get tape_list info'], [], [])
        return
    volume_ticket, (err1, warn1, info1) = get_volumedb_info(vol)
    if err1 or warn1:
        errors_and_warnings(vol, ['can not get inquire_vol info'], [], [])
        return
    
    tape_list = tape_ticket['tape_list']
    for i in range(len(tape_list)):
        #First check if there are files with matching locations on this tape.
        for j in range(len(tape_list)):
            if i == j:
                #Skip the current file.
                continue
            if tape_list[i]['location_cookie'] == \
                   tape_list[j]['location_cookie']:
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
                if tape_list[i]['deleted'] in  ["yes", "unknown"] or \
                   tape_list[j]['deleted'] in ["yes", "unknown"]:
                    #This is a possible situation:
                    # 1) The file clerk assigns a new bfid.
                    # 2) The volume clerk fails to update the EOD cookie.
                    # 3) The error is propagated back to encp, resulting
                    #    in a failure of the transfer.
                    # 4) With the EOD cookie not updated the same position
                    #    on tape is used.  Since the first file was
                    #    deemed failed and marked deleted, we don't consider
                    #    reaching this part of the scan a problem.
                    continue
                #elif volume_ticket['library'].find("shelf") != -1:
                #    #If the volume is no longer available, we need to skip
                #    # this check.
                #    warn = [message,]
                else:
                    err = [message,]
                errors_and_warnings(vol, err, warn, info)
                break
        else:
            #Continue on with checking the bfid.
            check_bit_file(tape_list[i]['bfid'],
                           {'file_record' : tape_list[i],
                            'volume_record' : volume_ticket})
                
last_db_tried = ("", (-1, ""))
search_list = []

# check_bit_file(bfid) -- check file using bfid
#
# [1] get file record using bfid
# [2] get pnfsid and pnfs path
# [3] using pnfs prefix and pnfsid to find the real path
# [4] use real path to scan the file

def check_bit_file(bfid, bfid_info = None):
    global last_db_tried

    err = []
    warn = []
    info = []

    prefix = bfid

    if not bfid:
        err.append("no bifd given")
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

    if file_record['deleted'] == "unknown":
        info.append("deleted=unknown")
        errors_and_warnings(prefix, err, warn, info)
        return

    #Determine if this file is a multiple copy.
    original_bfid = infc.find_original(bfid).get('original', None)
    if original_bfid != None and bfid != original_bfid:
        is_multiple_copy = True
    else:
        is_multiple_copy = False

    #Determine if this file has been migrated/duplicated.  (Some early,
    # volumes were set to readonly instead of 'migrating', so include those
    # volumes.)  Just because the variable name is is_migrated_copy,
    # any cloned or duplicated files will also have this set true.
    vol_state = volume_record['system_inhibit'][1]
    if volume_record and \
       (enstore_functions2.is_migration_state(vol_state) or \
        vol_state == 'readonly'):
        
        response_dict = infc.find_migrated(bfid)
        if not e_errors.is_ok(response_dict):
            # We should simply be able to give the following warning if we
            # get this far.  However, to allow this scan to work on systems
            # that do not have the updated information server with
            # find_migrated(), we need guess that if the system inhibit
            # says that the migration/duplication/cloning is done, that
            # we should just list the bfid as a migrated bfid.
            #      info.append("unable to determine migration status")
            if enstore_functions2.is_migrated_state(vol_state):
                src_bfids = [bfid]
            else:
                src_bfids = [] #If nothing is found, an empty list is returned.
        else:
            src_bfids = response_dict['src_bfid']
        if src_bfids and bfid in src_bfids:
            is_migrated_copy = True
        else:
            is_migrated_copy = False
    else:
        is_migrated_copy = False

    # we can not simply skip deleted files
    #
    # for each deleted file, we have to make sure:
    #
    # [1] no pnfsid, or
    # [2] no valid pnfsid, or
    # [3] in reused pnfsid case, the bfids are not the same
    if file_record['deleted'] == 'yes':
        info = info + ["deleted"]
    if is_migrated_copy and file_record['deleted'] == 'yes':
        #The file is migrated.  The file was already deleted (and
        # --with-deleted was used) or the newly migrated to copy has been
        # scanned.  Either way there is no error.
        errors_and_warnings(prefix, err, warn, info)
        return
    if file_record['deleted'] in ["yes", "unknown"] and \
       not file_record['pnfsid']:
        #The file is deleted, no pnfs id was recorded.  Not an error,
        # so move on to the next file.
        errors_and_warnings(prefix, err, warn, info)
        return

    #Loop over all found mount points.
    try:
        #Obtain the current path.  There are many ways to try and find the
        # file.  The original pathname is a good start, but there are a
        # lot of things to try before resorting to get_path().
        pnfs_path = find_pnfs_file.find_pnfsid_path(file_record['pnfsid'],
                                                    bfid,
                                                    file_record = file_record)
    except (OSError, IOError), msg:
        if msg.errno == errno.ENOENT and file_record['deleted'] in ['yes',
                                                                    'unknown']:
            # There is no error here.  Everything agrees the file is gone.
            pass
        elif file_record['deleted'] in ['yes', 'unknown'] and \
            msg.errno == errno.EEXIST and \
            msg.args[1] in ["replaced with newer file",
                            "replaced with another file",
                            "found original of copy"]:
            # The bfid is not active, and it is not active in pnfs.
            info.append(msg.args[1])
        else:
            err.append(msg.args[1])
        errors_and_warnings(prefix, err, warn, info)
        return
    except (ValueError,), msg:
        err.append(str(msg))
        errors_and_warnings(prefix, err, warn, info)
        return
        
    #Stat the file.
    f_stats, (e2, w2, i2) = get_stat(pnfs_path)
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

    #If we are a supper user, reset the effective uid and gid.
    if os.getuid() == 0:
        if os.geteuid() != f_stats[stat.ST_UID]:
            if os.geteuid() != 0:
                #If the currect effective ids are not currently root,
                # we need to set them back before (re)setting them.
                try:
                    os.setegid(0)
                    os.seteuid(0)
                except OSError:
                    pass

            #Set the uid and gid to match that of the file's owner.
            try:
                os.setegid(f_stats[stat.ST_GID])
                os.seteuid(f_stats[stat.ST_UID])
            except OSError:
                pass

    file_info = {"f_stats"       : f_stats,
                 "layer1"        : bfid, #layer1_bfid,
                 "file_record"   : file_record,
                 "pnfsid"        : file_record['pnfsid'],
                 "is_multiple_copy" : is_multiple_copy,
                 "volume_record" : volume_record,
                 "is_migrated_copy" : is_migrated_copy,
                 }

    e1, w1, i1 = check_file(pnfs_path, file_info)
    err = err + e1
    warn = warn + w1
    info = info + i1
    errors_and_warnings(prefix+' '+pnfs_path, err, warn, info)
    return

def check_file(f, file_info):

    f_stats = file_info['f_stats']
    bfid = file_info.get('layer1', None)
    filedb = file_info.get('file_record', None)
    #pnfs_id = file_info.get('pnfsid', None)
    is_multiple_copy = file_info.get('is_multiple_copy', None)
    volumedb = file_info.get('volume_record', None)
    is_migrated_copy = file_info.get('is_migrated_copy', None)

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

    #Get the correct/current pnfsid for this file.
    pnfs_id = get_pnfsid(f)[0]

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
    afn = os.path.join(os.path.dirname(f), ".(access)(%s)" % (pnfs_id,))

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

    #Check to make sure that PNFS is returning the same information
    # when getting layer 1 from the pnfsid and from the name.  There is
    # one known case that this has happened.
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

    #Check to make sure that PNFS is returning the same information
    # when getting layer 2 from the pnfsid and from the name.  There is
    # one known case that this has happened.
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

    #Check to make sure that PNFS is returning the same information
    # when getting layer 4 from the pnfsid and from the name.  There is
    # one known case that this has happened.
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

    #Look for missing pnfs information.
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
            # bfid was found in the pnfs layers.
            if bfid == layer4['bfid'] and is_deleted != "no":
                err.append("pnfs entry exists")

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
                if ffbp['pnfsid'] in ["", None, "None"]:
                    #err.append('no pnfs id in db')
                    break
                
                try:
                    p = pnfs.Pnfs(f)
                    cur_pnfsid = p.get_id(f) #pnfs of current searched file
                    unused = p.get_path(ffbp['pnfsid'],
                                        os.path.dirname(f))

                    #Deal with multiple possible matches.
                    if len(unused) != 1:
                        err.append("to many matches %s" % (unused,))
                        return err, warn, info

                    rm_pnfs = False
                except (OSError, IOError), msg:
                    if msg.args[0] == errno.ENOENT:
                        rm_pnfs = True
                    else:
                        rm_pnfs = None  #Unknown
                except (ValueError,), msg:
                    rm_pnfs = None #Unknown

                if ffbp['deleted'] == "yes":
                    marked_deleted = True
                elif ffbp['deleted'] == "no":
                    marked_deleted = False
                else:
                    marked_deleted = None

                if marked_deleted and rm_pnfs:
                    description = "deleted file"
                elif marked_deleted != None and not marked_deleted \
                         and rm_pnfs != None and not rm_pnfs:
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
                             cur_pnfsid))
                
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
        p_lc = string.split(layer4['location_cookie'], ':')
        f_lc = string.split(filedb['location_cookie'], ':')
        if layer4['location_cookie'] == filedb['location_cookie'] \
               and charset.is_in_filenamecharset(p_lc[0]) \
               and p_lc[1].isdigit():
            #This is a valid disk mover.
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
                # Report if Enstore DB and the dCache size in PNFS layer 2
                # are not the same.
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
    except (TypeError, ValueError, IndexError, AttributeError):
        err.append('no or corrupted drive')

    # CRC
    try:
        if layer4.get('crc', "") != "": # some do not have this field
            if long(layer4['crc']) != long(filedb['complete_crc']):
                # Report if Enstore DB and the Enstore CRC in PNFS layer 4
                # are not the same.
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
                # Report if Enstore DB and the dCache CRC in PNFS layer 2
                # are not the same.
                err.append('dcache_crc(%s, %s)' % (layer2['crc'],
                                                   crc_1_seeded))
    except (TypeError, ValueError, IndexError, AttributeError):
        err.append('no or corrupted CRC')

    # path
    try:
        layer4_name = get_enstore_pnfs_path(layer4['original_name'])
        current_name = get_enstore_pnfs_path(f)
        filedb_name = get_enstore_pnfs_path(filedb['pnfs_name0'])
        if layer4['original_name'] != filedb['pnfs_name0']: #layer4 vs filedb
            if is_multiple_copy and layer4_name == filedb_name:
                #If the corrected paths match, then there really isn't
                # any problem.
                pass
            else:
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
        err.append('no or corrupted pnfs_path')

    # pnfsid
    try:
        if pnfs_id != layer4['pnfsid'] or pnfs_id != filedb['pnfsid']:
            err.append('pnfsid(%s, %s, %s)' % (layer4['pnfsid'], pnfs_id,
                                               filedb['pnfsid']))
    except (TypeError, ValueError, IndexError, AttributeError):
        err.append('no or corrupted pnfsid')

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
                    alt_stats = os.stat(alt_path)

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

    if is_multiple_copy:
        info.append("is multiple copy")

    if is_migrated_copy:
        info.append("is migrated copy")

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
    line = os.path.abspath(line.strip())

    #Sanity check incase of user error specifying a non-pnfs path.
    if not pnfs.is_pnfs_path(line, check_name_only = 1):
        error(line+' ... not a pnfs file or directory')
        return

    """
    import profile
    import pstats
    profile.run("check(line)", "/tmp/scanfiles_profile")
    p = pstats.Stats("/tmp/scanfiles_profile")
    p.sort_stats('cumulative').print_stats(100)
    """

    check(line)

    for a_thread in ts_check:
        a_thread.join()
        result =  a_thread.get_result()
        try:
            err_j, warn_j, info_j = result #Why do we do this?
        except TypeError:
            pass #???
        del ts_check[0]

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

        option.Interface.__init__(self, args=args, user_mode=user_mode)


    def valid_dictionaries(self):
        return (self.help_options, self.scanfile_options)
    
    #  define our specific parameters
    parameters = ["[target_path [target_path_2 ...]]"] 

    scanfile_options = {
        option.BFID:{option.HELP_STRING:"treat input as bfids",
                         option.VALUE_USAGE:option.IGNORED,
                         option.DEFAULT_VALUE:option.DEFAULT,
                         option.DEFAULT_TYPE:option.INTEGER,
                         option.USER_LEVEL:option.USER},
        option.FILE_THREADS:{option.HELP_STRING:"Number of threads in files.",
                         option.VALUE_USAGE:option.REQUIRED,
                         option.VALUE_TYPE:option.INTEGER,
                         option.USER_LEVEL:option.USER,},
        option.INFILE:{option.HELP_STRING:"Use the contents of this file"
                       " as a list of targets to scan.",
                         option.VALUE_USAGE:option.REQUIRED,
                         option.VALUE_TYPE:option.STRING,
                         option.USER_LEVEL:option.USER,},
        option.PROFILE:{option.HELP_STRING:"Display profile info on exit.",
                            option.VALUE_USAGE:option.IGNORED,
                            option.USER_LEVEL:option.ADMIN,},
        option.VOL:{option.HELP_STRING:"treat input as volumes",
                         option.VALUE_USAGE:option.IGNORED,
                         option.DEFAULT_VALUE:option.DEFAULT,
                         option.DEFAULT_TYPE:option.INTEGER,
                         option.USER_LEVEL:option.USER},
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

    
    #number of threads to use for checking files.
    #AT_ONE_TIME = max(1, min(intf_of_scanfiles.file_threads, 10))

    process_mtab() #Do this once to cache the mtab info.

    try:

        #When the entire list of files/directories is listed on the command
        # line we need to loop over them.
        if file_list:
            for line in file_list:
                if line[:2] != '--':
                    if intf_of_scanfiles.bfid:
                        check_bit_file(line)
                    elif intf_of_scanfiles.vol:
                        check_vol(line)
                    else:
                        start_check(line)
                    
        #When the list of files/directories is of an unknown size from a file
        # object; read the filenames in one at a time for resource efficiency.
        elif file_object:
            line = file_object.readline()
            while line:
                line = line.split(" ... ")[0].strip()
                if intf_of_scanfiles.bfid:
                    check_bit_file(line)
                elif intf_of_scanfiles.vol:
                    check_vol(line)
                else:
                    start_check(line)
                line = file_object.readline()

    except (KeyboardInterrupt, SystemExit):
        #If the user does Control-C don't traceback.
        pass

    
def do_work(intf_of_scanfiles):
    global infc
    global lm

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

    if intf_of_scanfiles.profile:
        import profile
        import pstats
        profile.run("main(intf_of_scanfiles, file_object, file_list)", "/tmp/scanfiles_profile")
        p = pstats.Stats("/tmp/scanfiles_profile")
        p.sort_stats('cumulative').print_stats(100)
    else:
        main(intf_of_scanfiles, file_object, file_list)


if __name__ == '__main__':

    intf_of_scanfiles = ScanfilesInterface(sys.argv, 0) # zero means admin

    do_work(intf_of_scanfiles)
