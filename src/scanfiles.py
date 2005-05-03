#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

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

import info_client
import configuration_client
import option
import pnfs
import volume_family
import e_errors
import Trace
import charset
#from encp import e_access  #for e_access().

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
            sys.stderr.write("NEW COUNT DIFFERENCE: %s - %s = %s\n"
                        % (new_len, old_len, new_len - old_len))
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
    if absolute_path[:13] == "/pnfs/fs/usr/":
        return os.path.join("/pnfs/", absolute_path[13:])
    elif absolute_path[:6] == "/pnfs/":
        return absolute_path
    else:
        return absolute_path
    
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

def layer_file(f, n):
    pn, fn = os.path.split(f)
    return os.path.join(pn, ".(use)(%d)(%s)" % (n, fn))

def id_file(f):
    pn, fn = os.path.split(f)
    return os.path.join(pn, ".(id)(%s)" % (fn, ))

def parent_file(f, pnfsid = None):
    pn, fn = os.path.split(f)
    if pnfsid:
        return os.path.join(pn, ".(parent)(%s)" % (pnfsid))
    else:
        fname = id_file(f)
        f = open(fname)
        pnfsid = f.readline()
        f.close()
        return os.path.join(pn, ".(parent)(%s)" % (pnfsid))


def get_layer_1(f):

    err = []
    warn = []
    info = []

    # get bfid from layer 1
    try:
        f1 = open(layer_file(f, 1))
        bfid = f1.readline()
        f1.close()
    except (OSError, IOError), detail:
        bfid = None
        if detail.errno == errno.EACCES or detail.errno == errno.EPERM:
            err.append('no read permissions for layer 1')
        else:
            err.append('corrupted layer 1 metadata')

    return bfid, (err, warn, info)

def get_layer_2(f):

    err = []
    warn = []
    info = []

    # get dcache info from layer 2
    try:
        f2 = open(layer_file(f, 2))
        layer2 = f2.readlines()
        f2.close()
    except (OSError, IOError), detail:
        layer2 = None
        if detail.errno == errno.EACCES or detail.errno == errno.EPERM:
            err.append('no read permissions for layer 2')
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
        f4 = open(layer_file(f, 4))
        layer4 = f4.readlines()
        f4.close()

        l4 = {}
        if layer4:
            l4 = {'volume' : layer4[0].strip(),
                  'location_cookie' : layer4[1].strip(),
                  'size' : layer4[2].strip(),
                  'file_family' : layer4[3].strip(),
                  'original_name' : layer4[4].strip(),
                  # map file no longer used
                  'pnfsid' : layer4[6].strip(),
                  # map pnfsid no longer used
                  'bfid' : layer4[8].strip(),
                  }

        try:
            l4['drive'] = layer4[9].strip()
        except IndexError:
            pass
        try:
            l4['crc'] = layer4[10].strip()
        except IndexError:
            pass

    except (KeyboardInterrupt, SystemExit), msg:
        raise msg
    except (OSError, IOError), msg:
        #pf = None
        l4 = None
        if msg.errno == errno.EACCES or msg.errno == errno.EPERM:
            err.append('no read permissions for layer 4')
        else:
            err.append('corrupted layer 4 metadata')

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
    if fr['status'][0] == e_errors.NO_FILE:
        err.append('not in db')
    elif not e_errors.is_ok(fr):
        err.append('file db error (%s)' % (fr['status'],))
    else:
        # Look for missing file database information.
        if not fr.has_key('pnfs_name0'):
            err.append('no filename in db')
        if not fr.has_key('pnfsid'):
            err.append('no pnfs id in db')

    return fr, (err, warn, info)

def get_stat(f):
    __pychecker__="unusednames=i"

    err = []
    warn = []
    info = []

    ### We need to try a few times.  There are situations where the server
    ### is busy and the lack of responce looks like a 'does not exist' responce.
    ### This can lead 'invalid directory entry' situation, but in reality
    ### it is a false negative.
    
    #Do one stat() for each file instead of one for each os.path.isxxx() call.
    for i in range(5):
        try:
            f_stats = os.lstat(f)

            #On success return immediatly.
            return f_stats, (err, warn, info)
        except OSError:
            time.sleep(0.25) #Sleep for a quarter of a second.
            continue
            
    try:
        f_stats = os.lstat(f)
    except OSError, msg:
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
            err.append("permission error")
            return None, (err, warn, info)

        err.append(os.strerror(msg.errno))
        return None, (err, warn, info)

    return f_stats, (err, warn, info)

def get_pnfsid(f):

    err = []
    warn = []
    info = []
    
    #Get the id of the parent directory.
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
        f_stats, (err, warn, info) = get_stat(f)

    if err or warn:
        errors_and_warnings(f, err, warn, info)
        return
    
    file_info = {"f_stats"       : f_stats}

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
            if err or warn:
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

    # skip volmap and .bad and .removed directory
    fname = os.path.basename(d)
    if fname == 'volmap' or fname[:3] == '.B_' or fname[:3] == '.A_' \
           or fname[:8] == '.removed':
        return err, warn, info
        
    if check_permissions(d_stats, os.R_OK | os.X_OK):
        err, warn, info = check_parent(d)
        if err or warn:
            return err, warn, info

        #Get the list of files.
        file_list = os.listdir(d)

        for i in range(0, len(file_list)):
            
            f = os.path.join(d, file_list[i])

            check(f)

    else:
        err.append("can not access directory")
    
    return err, warn, info


def check_file(f, file_info):

    f_stats = file_info['f_stats']

    err = []
    warn = []
    info = []

    fname = os.path.basename(f)

    #If the file is an (P)NFS or encp temporary file, give the error that
    # it still exists.
    if fname[:4] == ".nfs" or fname[-5:] == "_lock":
        err.append("found temporary file")
        return err, warn, info

    #Skip blacklisted files.
    if fname[:4] == '.bad':
        info.append("marked bad")
        return err, warn, info #Non-lists skips any output.

    #Get information from the layer 1 and layer 4.
    bfid, (err1, warn1, info1) = get_layer_1(f)
    layer4, (err4, warn4, info4) = get_layer_4(f)

    err = err + err1 + err4
    warn = warn + warn1 + warn4
    info = info + info1 + info4
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
            #Get the info from layer 2.
            layer2 = get_layer_2(f)[0]

            #If the size from stat(1) and layer 2 are both zero, then the
            # file really is zero length and the dCache did not forward
            # the file to tape/Enstore.
            if f_stats[stat.ST_SIZE] == 0L and layer2.get('size', None) == 0L:
                info.append("zero length dCache file not on tape")
                return err, warn, info
            elif f_stats[stat.ST_SIZE] == 0 and not layer2:
                err.append('missing file')
            else:
                if len(bfid) < 8:
                    err.append('missing layer 1')

                if not layer4.has_key('bfid'):
                    err.append('missing layer 4')

                if layer2['pools']:
                    info.append("pools(%s)" % (layer2['pools'],))
                
    if err or warn:
        return err, warn, info

    # Get file database information.
    filedb, (err_f, warn_f, info_f) = get_filedb_info(bfid)
    # Get file database errors.
    err = err + err_f
    warn = warn + warn_f
    info = info + info_f
    if err or warn:
        return err, warn, info

    # volume label
    try:
        if layer4['volume'] != filedb['external_label']:
            err.append('label(%s, %s)' % (layer4['volume'],
                                          filedb['external_label']))
    except (TypeError, ValueError, IndexError, AttributeError):
        err.append('no or corrupted external_label')
        
    # location cookie
    try:
        #The location cookie is split into three sections.  All but the eariest
        # files use only the last of these three sections.  Thus, this check
        # makes sure that (1) the length of both original strings are the same
        # and (2) only the last section matches exactly.
        p_lc = string.split(layer4['location_cookie'], '_')[2]
        f_lc = string.split(filedb['location_cookie'], '_')[2]
        if p_lc != f_lc or \
               len(layer4['location_cookie']) != len(filedb['location_cookie']):
            err.append('location_cookie(%s, %s)'%(layer4['location_cookie'],
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
                    err.append('error finding vol' + filedb['external_label'])
                return err, warn, info
            file_family = volume_family.extract_file_family(vol['volume_family'])
            library = vol['library']
            vol_info[filedb['external_label']] = {}
            vol_info[filedb['external_label']]['ff'] = file_family
            vol_info[filedb['external_label']]['lm'] = library

        # File Family check.  Take care of MIGRATION, too.
        if layer4['file_family'] != file_family and \
            layer4['file_family'] + '-MIGRATION' != file_family:
            err.append('file_family(%s, %s)' % (layer4['file_family'],
                                                file_family))
        # Library Manager check.
        if library not in lm:
            err.append('no such library (%s)' % (library))
    except (TypeError, ValueError, IndexError, AttributeError):
        err.append('no or corrupted file_family')
        
    # drive
    try:
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
                err.append("parent_id(%s, %s)" % (parent_id, parent_dir_id))
        
    except (TypeError, ValueError, IndexError, AttributeError):
        err.append('no or corrupted parent id')

    # deleted
    try:
        if filedb['deleted'] != 'no':
            err.append('deleted(%s)' % (filedb['deleted']))
    except (TypeError, ValueError, IndexError, AttributeError):
        err.append('no deleted field')

        
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
    
class ScanfilesInterface(option.Interface):
    def __init__(self, args=sys.argv, user_mode=0):

        self.infile = None

        self.file_threads = 3
        self.directory_threads = 1

        option.Interface.__init__(self, args=args, user_mode=user_mode)


    def valid_dictionaries(self):
        return (self.help_options, self.scanfile_options)
    
    #  define our specific parameters
    parameters = ["[target_path [target_path_2 ...]]"] 

    scanfile_options = {
        option.INFILE:{option.HELP_STRING:"Use the contents of this file"
                       " as a list of targets to scan.",
                         option.VALUE_USAGE:option.REQUIRED,
                         option.VALUE_TYPE:option.STRING,
                         option.USER_LEVEL:option.USER,},
        option.FILE_THREADS:{option.HELP_STRING:"Number of threads in files.",
                         option.VALUE_USAGE:option.REQUIRED,
                         option.VALUE_TYPE:option.INTEGER,
                         option.USER_LEVEL:option.USER,},
        }

def handle_signal(sig, frame):
    __pychecker__ = "unusednames=sig,frame"
    global threads_stop

    #Tell other threads to stop now.
    stop_threads_lock.acquire()
    threads_stop = True
    stop_threads_lock.release()

    sys.exit(1)
    
if __name__ == '__main__':

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    intf_of_scanfiles = ScanfilesInterface(sys.argv, 0) # zero means admin

    if intf_of_scanfiles.help:
        usage()
        sys.exit(0)

    if intf_of_scanfiles.infile:
        file_object = open(intf_of_scanfiles.infile)
        file_list = None
    elif len(sys.argv) == 1:
        file_object = sys.stdin
        file_list = None
    else:
        file_object = None
        file_list = sys.argv[1:]

    csc = configuration_client.ConfigurationClient(
        (intf_of_scanfiles.config_host,
         intf_of_scanfiles.config_port))

    #Get the list of library managers.
    lm = csc.get_library_managers().keys()

    infc = info_client.infoClient(csc)

    #number of threads to use for checking files.
    #AT_ONE_TIME = max(1, min(intf_of_scanfiles.file_threads, 10))
    
    try:

        #When the entire list of files/directories is listed on the command
        # line we need to loop over them.
        if file_list:
            for line in file_list:
                if line[:2] != '--':
                    start_check(line)
                    
        #When the list of files/directories is of an unknown size from a file
        # object; read the filenames in one at a time for resource efficiency.
        elif file_object:
            line = file_object.readline()
            while line:
                start_check(line)
                line = file_object.readline()

    except (KeyboardInterrupt, SystemExit):
        #If the user does Control-C don't traceback.
        pass
