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
import encp
import enstore_constants

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
external_transitions = {} #Ttranslate /pnfs/sam/lto to /pnfs/fs/usr/sam-lto

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
    
    i = 0
    while i < 3:
        # get info from layer
        try:
            fl = open(layer_filename)
            layer_info = fl.readlines()
            fl.close()
            break
        except (OSError, IOError), detail:
            if detail.args[0] in [errno.EACCES, errno.EPERM] and os.getuid() == 0:
                #If we get here and the real id is user root, we need to reset
                # the effective user id back to that of root ...
                os.seteuid(0)
                os.setegid(0)
            else:
                #If the problem wasn't permissions, lets give the system a
                # moment to catch up.
                time.sleep(0.1)

            i = i + 1
            continue

    else:
        raise detail

    return layer_info

def get_layer_1(f):
    err = []
    warn = []
    info = []

    # get bfid from layer 1
    try:
        bfid = get_layer(layer_file(f, 1))
    except (OSError, IOError), detail:
        bfid = None
        if detail.errno in [errno.EACCES, errno.EPERM]:
            err.append('no read permissions for layer 1')
        elif detail.args[0] in [errno.ENOENT, errno.EISDIR]:
            pass
        else:
            err.append('corrupted layer 1 metadata')

    try:
        bfid = bfid[0].strip()
    except:
        bfid = ""

    return bfid, (err, warn, info)


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
            l2['hsm'] = hsm_match.search(line2).group().split("=")[1]
        except AttributeError:
            l2['hsm'] = None

        try:
            crc_match = re.compile("c=[1-9]+:[a-zA-Z0-9]{8}")
            l2['crc'] = long(crc_match.search(line2).group().split(":")[1], 16)
        except AttributeError:
            l2['crc'] = None

        try:
            size_match = re.compile("l=[0-9]+")
            l2['size'] = long(size_match.search(line2).group().split("=")[1])
        except AttributeError:
            l2['size'] = None

        l2['pools'] = []
        for item in layer2[2:]:
            l2['pools'].append(item.strip())

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

    return l4, (err, warn, info)

def get_layers(f):
    bfid, (err, warn, info) = get_layer_1(f)
    layer4, (err1, warn1, info1) = get_layer_4(f)

    return (bfid, layer4), (err + err1, warn + warn1, info + info1)

def get_filedb_info(bfid):
    
    err = []
    warn = []
    info = []

    if not bfid:
        return None, (err, warn, info)
    
    # Get file database information.
    fr = infc.bfid_info(bfid)
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
    
    return fr, (err, warn, info)

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
        
    for database_info, (db_num, mp) in db_pnfsid_cache.items():
        if db_num == 0 or os.path.basename(mp) == "fs":
            #For /pnfs/fs we need to find all of the /pnfs/fs/usr/* dirs.
            p = pnfs.Pnfs()
            use_path = os.path.join(mp, "usr")
            for dname in os.listdir(use_path):
                tmp_name = os.path.join(use_path, dname)
                if not os.path.isdir(tmp_name):
                    continue
                tmp_db_info = p.get_database(os.path.join(use_path, dname)).strip()
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
    if fname == 'volmap' or fname[:3] == '.B_' or fname[:3] == '.A_' \
           or fname[:8] == '.removed' or fname.lower().find("migration") != -1:
        return err, warn, info
        
    if check_permissions(d_stats, os.R_OK | os.X_OK):
        err, warn, info = check_parent(d)
        if err or warn:
            return err, warn, info

        #Get the list of files.
        file_list = os.listdir(d)

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
    tape_ticket = infc.tape_list(vol)
    if not e_errors.is_ok(tape_ticket):
        errors_and_warnings(vol, ['can not get info'], [], [])
        return
    
    tape_list = tape_ticket['tape_list']
    for i in range(len(tape_list)):
        #First check if there are files with matching locations on this tape.
        for j in range(len(tape_list)):
            if i == j:
                #Skip the current file.
                continue
            if tape_list[i]['location_cookie'] == tape_list[j]['location_cookie']:
                #If we get here then we have multiple locations for the
                # same tape.
                ### Historical Note: This has been found to have happened
                ### while importing SDSS DLT data into Enstore.
                err = ['volume %s has duplicate location %s (%s, %s)' %
                       (vol, tape_list[i]['location_cookie'],
                        tape_list[i]['bfid'], tape_list[j]['bfid'])]
                errors_and_warnings(vol, err, [], [])
                break
        else:
            #Continue on with checking the bfid.
            check_bit_file(tape_list[i]['bfid'],
                           {'file_record' : tape_list[i]})
                
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

    #If this gets set to True later on, then this bfid points to a multiple
    # copy.  Some checks need to be skipped if true.
    is_multile_copy = False

    if not bfid:
        err.append("no bifd given")
        errors_and_warnings(prefix, err, warn, info)
        return

    if bfid_info:
        file_record = bfid_info.get('file_record', None)
    else:
        file_record = None

    if not file_record:
        file_record, (err, warn, info) = get_filedb_info(bfid)
        if err or warn:
            errors_and_warnings(prefix, err, warn, info)
            return

    prefix = string.join([prefix, "...", file_record['external_label'],
                          file_record['location_cookie']], " ")

    if file_record['deleted'] == "unknown":
        info.append("deleted=unkown")
        errors_and_warnings(prefix, err, warn, info)
        return

    #We will need the pnfs database numbers.
    if pnfs.is_pnfsid(file_record['pnfsid']):
        pnfsid_db = int(file_record['pnfsid'][:4], 16)
    else:
        pnfsid_db = None

    # we can not simply skip deleted files
    #
    # for each deleted file, we have to make sure:
    #
    # [1] no pnfsid, or
    # [2] no valid pnfsid, or
    # [3] in reused pnfsid case, the bfids are not the same
    if file_record['deleted'] == 'yes':
        info = info + ["deleted"]

    #Loop over all found mount points.
    possible_reused_pnfsid = 0
    for database_info, (db_num, mp)  in [last_db_tried] + search_list:

        #If last_db_tried is still set to its initial value, we need to
        # skip the the next.
        if db_num < 0:
            continue
        
        #This test is to make sure that the pnfs filesystem we are going
        # to query has a database N (where N is pnfsid_db).  Otherwise
        # we hang querying a non-existant database.  If the pnfsid_db
        # matches the lat one we tried we skip this test as it has
        # already been done.
        if last_db_tried[0] != pnfsid_db:
            try:
                pnfs.N(pnfsid_db, mp).get_databaseN()
            except IOError:
                #This /pnfs/fs doesn't contain a database with the id we
                # are looking for.
                continue
        
        #We don't need to determine the full path of the file
        # to know if it exists.  The path could be different
        # between two machines anyway.
        afn = access_file(mp, file_record['pnfsid']) #afn =  access file name

        #Check layer 1 to get the bfid.
        layer1_bfid, (err_l, warn_l, info_l) = get_layer_1(afn)
        err = err + err_l
        warn = warn + warn_l
        info = info + info_l
        if err or warn:
            errors_and_warnings(prefix, err, warn, info)
            return
        if layer1_bfid:
            #Make sure this is the correct file.
            if layer1_bfid == file_record['bfid']:
                if file_record['deleted'] == 'yes':
                    try:
                        tmp_name_list = pnfs.Pnfs(shortcut = True).get_path(file_record['pnfsid'], mp)
                        #Deal with multiple possible matches.
                        if len(tmp_name_list) == 1:
                            err.append("pnfs entry exists")
                        else:
                            err.append("to many matches %s" % tmp_name_list)
                    except (OSError, IOError), detail:
                        if detail.errno in [errno.EBADFD, errno.EIO]:
                            err.append("%s orphaned file" % (file_record['pnfsid'],))
                        else:
                            err.append("%s error accessing file"%(file_record['pnfsid'],))
                        
                    errors_and_warnings(prefix, err, warn, info)
                    return
                else:
                    #p = pnfs.Pnfs()
                    #db_a_dirpath = pnfs.get_directory_name(afn)
                    #db_info = p.get_database(db_a_dirpath)
                    db_info = get_database(afn)

                    #Update the global cache information.
                    if database_info != db_info:

                        #At this point the database that the file is in
                        # doesn't match the one that returned a positive hit.
                        # So, we first check if a known PNFS database is
                        # a match...
                        for item in search_list:
                            if item[0] == db_info:
                                pnfs_path = access_file(item[1][1],
                                                        file_record['pnfsid'])
                                layer1_bfid, unused = get_layer_1(pnfs_path)
                                if layer1_bfid and \
                                       layer1_bfid == file_record['bfid']:
                                    last_db_tried = copy.copy(item)
                                    break
                        else:
                            #...if it is not a match then we have a database
                            # not it our current cached list.  So we need
                            # to find it.  This is going to be a resource
                            # hog, but once we find it, we won't need to
                            # do so for any more files.
                            
                            #If this p.get_path() fails, it is most likely,
                            # because of permission problems of either
                            # /pnfs/fs or /pnfs/fs/usr.  Especially for
                            # new pnfs servers.
                            try:
                                p = pnfs.Pnfs()
                                pnfs_path_list = p.get_path(
                                    file_record['pnfsid'], mp)
                                #Deal with multiple possible matches.
                                if len(pnfs_path_list) == 1:
                                    pnfs_path = pnfs_path_list[0]
                                else:
                                    err.append("to many matches %s" %
                                               (pnfs_path_list,))
                                    errors_and_warnings(prefix, err, warn, info)
                                    return
                                pnfsid_mp = p.get_pnfs_db_directory(pnfs_path)
                            except (OSError, IOError):
                                pnfsid_mp = None
                                pnfs_path = afn

                            if pnfsid_mp != None:
                                #This is just some paranoid checking.
                                afn = access_file(pnfsid_mp,
                                                  file_record['pnfsid'])
                                layer1_bfid, unused = get_layer_1(afn)
                                if layer1_bfid and \
                                       layer1_bfid == file_record['bfid']:
                                    last_db_tried = (db_info, (pnfsid_db, pnfsid_mp))
                                    add_mtab(db_info, pnfsid_db, pnfsid_mp)
                            else:
                                last_db_tried = ("", (-1, ""))

                    else:
                        last_db_tried = (db_info, (pnfsid_db, mp))
                        #We found the file, set the pnfs path.
                        pnfs_path = afn

                    #pnfs_path needs to be set correctly by this point.
                    break
            elif infc.find_original(file_record['bfid'])['original'] == \
                     layer1_bfid:
                pnfs_path = afn
                is_multile_copy = True
                break

            #If we found the right bfid brand, we know the right pnfs system
            # was found.
            pnfs_brand = encp.extract_brand(layer1_bfid)
            filedb_brand = encp.extract_brand(file_record['bfid'])
            if pnfs_brand and filedb_brand and pnfs_brand == filedb_brand:
                if file_record['deleted'] == 'yes':
                    info.append("reused pnfsid")
                else:
                    #err.append("reused pnfsid")
                    ## Need to keep trying in case the wrong pnfs systems
                    ## pnfsid match was found.
                    possible_reused_pnfsid = possible_reused_pnfsid + 1
                    continue
                errors_and_warnings(prefix, err, warn, info)
                return

            #If we found a bfid that didn't have the correct id or the
            # brands did not match, go back to the top and try the next
            # pnfs filesystem.
            
    else:
        if file_record['deleted'] != 'yes':
            err.append("%s does not exist" % (file_record['pnfsid'],))

        if possible_reused_pnfsid > 0:
            err.append("reused pnfsid")

        layer1_bfid, unused = get_layer_1(file_record['pnfs_name0'])
        if layer1_bfid and layer1_bfid != file_record['bfid']:
            #If this is the case that the bfids don't match,
            # also include this piece of information.
            info.append("replaced with newer file")
            errors_and_warnings(prefix, err, warn, info)
            return

        errors_and_warnings(prefix, err, warn, info)
        return

    #Since we know if we are using /pnfs/fs or not, we can maniplate the
    # original name to the correct pnfs base path.  This will speed things
    # up when scanning files written to /pnfs/xyz but only having /pnfs/fs
    # mounted for the scan.
    #
    #This won't help for the case of moved or renamed files.  We still
    # will bite the bullet of calling get_path() for those.
    if not is_access_name(pnfs_path):
        #If we have already had to do a full path lookup to find the
        # correct pnfs database/mountpoint, we don't need to worry about
        # any of this path munging.
        use_name = pnfs_path
        use_mp = pnfsid_mp
    elif db_num == 0 and file_record['pnfs_name0'].find("/pnfs/fs/usr") == -1:
        use_name = get_dcache_pnfs_path(file_record['pnfs_name0'])
        use_mp = mp.replace("/pnfs/fs", "/pnfs/fs/usr/", 1)
    elif mp.find("/pnfs/fs/usr/") >= 0 and \
             file_record['pnfs_name0'].find("/pnfs/fs/usr") == -1:
        use_name = get_dcache_pnfs_path(file_record['pnfs_name0'])
        use_mp = mp
    elif mp.find("/pnfs/fs/usr/") == -1 and \
             file_record['pnfs_name0'].find("/pnfs/fs/usr") >= 0:
        use_name = get_enstore_pnfs_path(file_record['pnfs_name0'])
        use_mp = mp
    else:
        use_name = file_record['pnfs_name0']
        use_mp = mp
    
    use_name = os.path.abspath(use_name)
    use_mp = os.path.abspath(use_mp)

    ###
    for old_value, new_value in external_transitions.items():
        use_name = use_name.replace(old_value, new_value, 1)

    cur_pnfsid = get_pnfsid(use_name)[0]
    if not cur_pnfsid or cur_pnfsid != file_record['pnfsid']:
        #Before jumping off the deep end by calling get_path(), lets try
        # one more thing.  Here we try and remove any intermidiate
        # directories that are not present in the current path.
        #  Example: original path begins: /pnfs/sam/dzero/...
        #           current use path begins /pnfs/fs/usr/dzero/...
        #           If we can detect that we need to remove the "/sam/"
        #           part we can save the time of a full get_path() lookup.
        just_pnfs_path_part = pnfs.strip_pnfs_mountpoint(file_record['pnfs_name0'])
        dir_list = just_pnfs_path_part.split("/", 2) #Don't check everything...
        for i in range(len(dir_list[:-1])):
            single_dir = os.path.join(use_mp, dir_list[i])
            try:
                os.stat(single_dir)
                use_name = os.path.join(use_mp,
                                         string.join(dir_list[i:], "/"))
                break
            except (OSError, IOError):
                pass

        #We need to check if it is an orphaned file.  If the pnfsids match
        # then the file has not been moved or renamed since it was written.
        # If they match, then the ".(access)()" filename is passed to
        # check_file(). check_file() should skip its own check of the
        # ".(access)()" filenames, since they have been shone to still be
        # valid.
        #
        # If it doesn't match then:
        # 1) The file is orphaned.  (get_path() gives ENOENT or EIO)
        # 2) The file is moved.  (get_path() gives the new path)
        # 3) The file is renamed. (get_path() gives the new name)
        cur_pnfsid = get_pnfsid(use_name)[0]
        if not cur_pnfsid or cur_pnfsid != file_record['pnfsid']:
            #Since we have no idea in pnfs-land where we will be headed,
            # lets set things so that we will be able to have access
            # permissions set if possible.
            if os.getuid() == 0 and os.geteuid() != 0:
                try:
                    os.seteuid(0)
                    os.setegid(0)
                except OSError:
                    pass
            try:
                tmp_name_list = pnfs.Pnfs(shortcut = True).get_path(file_record['pnfsid'], use_mp)
                #Deal with multiple possible matches.
                if len(tmp_name_list) == 1:
                    tmp_name = tmp_name_list[0]
                else:
                    err.append("to many matches %s" % (tmp_name_list,))
                    errors_and_warnings(prefix, err, warn, info)
                    return
                
                if tmp_name[0] == "/":
                    #Make sure the path is a absolute path.
                    pnfs_path = tmp_name
                else:
                    #If the path is not an absolute path we get here.  What
                    # happend is that get_path() was able to find a pnfs
                    # mount point connected to the correct pnfs database,
                    # but not a mount for the correct database.
                    #
                    #The best we can do is use the .(access)() name.
                    pass
            except (OSError, IOError), detail:
                if detail.errno  in [errno.EBADFD, errno.EIO]:
                    err = err + ["%s orphaned file"%(file_record['pnfsid'])]
                    errors_and_warnings(prefix, err, warn, info)
                    return
                else:
                    err = err + ["%s error accessing file"%(file_record['pnfsid'])]
                    errors_and_warnings(prefix, err, warn, info)
                    return
        else:
            pnfs_path = use_name
    else:
        pnfs_path = use_name

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
                     "layer1"        : layer1_bfid,
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
                 "layer1"        : layer1_bfid,
                 "file_record"   : file_record,
                 "pnfsid"        : file_record['pnfsid'],
                 "is_multiple_copy" : is_multile_copy}

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
    pnfs_id = file_info.get('pnfsid', None)
    is_multiple_copy = file_info.get('is_multiple_copy', None)

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
        return err, warn, info #Non-lists skips any output.

    #Get the info from layer 2.
    layer2 = get_layer_2(f)[0]

    #If this file is not supposed to be forwarded to tape by dCache,
    # check the parent id and compare the file size from the os
    # and layer 2.
    if layer2.get('hsm', None) == "no":
        err_p, warn_p, info_p = check_parent(f)
        err = err + err_p
        warn = warn + warn
        info = info + info_p

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

        return err, warn, info
                
    #Get information from the layer 1 and layer 4 (if necessary).
    if not bfid:
        bfid, (err1, warn1, info1) = get_layer_1(f)
        err = err + err1
        warn = warn + warn1
        info = info + info1
    layer4, (err4, warn4, info4) = get_layer_4(f)

    err = err + err4
    warn = warn + warn4
    info = info + info4
    if err or warn:
        return err, warn, info

    #Look for missing pnfs information.
    try:
        if bfid != layer4['bfid']:
            err.append('bfid(%s, %s)' % (bfid, layer4['bfid']))
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

    # Get file database information.
    if not filedb:
        filedb, (err_f, warn_f, info_f) = get_filedb_info(bfid)
        # Get file database errors.
        err = err + err_f
        warn = warn + warn_f
        info = info + info_f
        if err or warn:
            return err, warn, info

    # volume label
    try:
        if not is_multiple_copy:
            if layer4['volume'] != filedb['external_label']:
                err.append('label(%s, %s)' % (layer4['volume'],
                                              filedb['external_label']))
    except (TypeError, ValueError, IndexError, AttributeError):
        err.append('no or corrupted external_label')
        
    # location cookie
    try:
        if not is_multiple_copy:
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
    except (TypeError, ValueError, IndexError, AttributeError):
        err.append('no or corrupted size')
        
    # file_family and library
    try:
        #Get the volume specific information.
        if vol_info.has_key(filedb['external_label']):
            file_family = vol_info[filedb['external_label']]['ff']
            library = vol_info[filedb['external_label']]['lm']
        else:
            vol = infc.inquire_vol(filedb['external_label'])
            if not e_errors.is_ok(vol['status']):  #[0] != e_errors.OK:
                if vol['status'][0] == e_errors.NO_VOLUME:
                    err.append('missing vol ' + filedb['external_label'])
                else:
                    err.append("error finding vol %s: (%s)" %
                               (filedb['external_label'], vol['status']))
                return err, warn, info
            file_family = volume_family.extract_file_family(vol['volume_family'])
            library = vol['library']
            vol_info[filedb['external_label']] = {}
            vol_info[filedb['external_label']]['ff'] = file_family
            vol_info[filedb['external_label']]['lm'] = library

        # File Family check.  Take care of MIGRATION, too.
        if not is_multiple_copy:
            if layer4['file_family'] != file_family and \
                layer4['file_family'] + '-MIGRATION' != file_family:
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
        if not is_multiple_copy:
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
                err.append('crc(%s, %s)' % (layer4['crc'],
                                            filedb['complete_crc']))
    except (TypeError, ValueError, IndexError, AttributeError):
        err.append('no or corrupted CRC')

    # path
    try:
        if layer4['original_name'] != filedb['pnfs_name0']: #layer 4 vs. file db
            #print layer 4, current name, file database.  ERROR
            err.append("filename(%s, %s, %s)" %
                       (layer4['original_name'], f, filedb['pnfs_name0']))
        elif is_access_name(f):
            #Skip the renamed and moved tests if the filename of of
            # the type ".(access)()".
            pass
        elif f != layer4['original_name']: # current pathname vs. layer 4
            layer4_name = get_enstore_pnfs_path(layer4['original_name'])
            current_name = get_enstore_pnfs_path(f)

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
        if not pnfs_id:
            pnfs_id = get_pnfsid(f)[0]
        
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
                except OSError:
                    alt_stats = None
                if f_stats != alt_stats:
                    err.append("parent_id(%s, %s)" % (parent_id, parent_dir_id))
        
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
        err_j, warn_j, info_j = a_thread.get_result()
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
        option.EXTERNAL_TRANSITIONS:{option.HELP_STRING:
                                     "User hints for directory searches. "
                                     "ie. --external_transitions "
                                     "sam/lto sam-lto",
                                     option.VALUE_NAME:"old_path",
                                     option.VALUE_USAGE:option.REQUIRED,
                                     option.VALUE_TYPE:option.LIST,
                                     option.USER_LEVEL:option.USER,

                                     option.EXTRA_VALUES:[{option.VALUE_NAME:"new_path",
                                          option.VALUE_TYPE:option.LIST,
                                          option.VALUE_USAGE:option.REQUIRED,},
                                         ],},
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

    #For processing certain storage_groups/mount_points.  This allows
    # the user to give scanfiles.py some hints to avoid performing
    # get_path() calls for every file.
    for i in range(len(intf_of_scanfiles.old_path)):
        external_transitions[intf_of_scanfiles.old_path[i]] = \
                                         intf_of_scanfiles.new_path[i]

    if intf_of_scanfiles.infile:
        file_object = open(intf_of_scanfiles.infile)
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
