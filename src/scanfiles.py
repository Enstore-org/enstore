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

import info_client
import option
import pnfs
import volume_family
import e_errors
from encp import e_access  #for e_access().

os.access = e_access   #Hack for effective ids instead of real ids.

infc = None
ff = {} #File Family cache.
ONE_DAY = 24*60*60

def usage():
    print "usage: %s [path [path2 [path3 [ ... ]]]]"%(sys.argv[0])
    print "usage: %s --help"%(sys.argv[0])
    print "usage: %s --infile file"%(sys.argv[0])

def error(s):
    print s, '... ERROR'

def warning(s):
    print s, '... WARNING'

def layer_file(f, n):
    pn, fn = os.path.split(f)
    return os.path.join(pn, '.(use)(%d)(%s)'%(n, fn))

def check(f):

    msg = []
    warn = []

    #If the file is an (P)NFS or encp temporary file, give the error that
    # it still exists.
    if os.path.basename(f)[:4] == ".nfs" \
           or os.path.basename(f)[-5:] == "_lock":
        msg.append("found temporary file")
        return msg, warn

    #Determine if the file exists and we can access it.
    try:
        f_stats = os.stat(f)
    except OSError:
        f = pnfs.get_local_pnfs_path(f)
        try:
            f_stats = os.stat(f)
        except OSError:
            #No read permission is the likeliest at this point...
            msg.append('no read permission')
            return msg, warn

    # get xref from layer 4 (?)
    try:
        pf = pnfs.File(f)
    except (KeyboardInterrupt, SystemExit), msg:
        raise msg
    except OSError:
        msg.append('corrupted layer 4 metadata')
        #return msg, warn

    # get bfid from layer 1
    try:
        f1 = open(layer_file(f, 1))
        bfid = f1.readline()
        f1.close()
    except OSError:
        msg.append('corrupted layer 1 metadata')

    if msg:
        return msg, warn

    #Look for missing pnfs information.
    try:
        if bfid != pf.bfid:
            msg.append('bfid(%s, %s)'%(bfid, pf.bfid))
    except (TypeError, ValueError, IndexError, AttributeError):
    	age = time.time() - f_stats[8]
        if age < ONE_DAY:
            warn.append('younger than 1 day (%d)'%(age))
            return msg, warn
        
        if len(bfid) < 8:
            msg.append('missing layer 1')

        if not hasattr(pf, 'bfid'):
            msg.append('missing layer 4')
            
        if msg or warn:
            return msg, warn

    # Get file database information.
    fr = infc.bfid_info(bfid)
    if fr['status'][0] != e_errors.OK:
        msg.append('not in db')
	return msg, warn

    # Look for missing file database information.
    if not fr.has_key('pnfs_name0'):
        msg.append('no filename in db')
    if not fr.has_key('pnfsid'):
        msg.append('no pnfs id in db')

    if msg or warn:
        return msg, warn

    #Compare pnfs metadata with file database metadata.
    
    # volume label
    try:
        if pf.volume != fr['external_label']:
            msg.append('label(%s, %s)'%(pf.volume, fr['external_label']))
    except (TypeError, ValueError, IndexError, AttributeError):
        msg.append('no or corrupted external_label')
        
    # location cookie
    try:
        # if pf.location_cookie != fr['location_cookie']:
        #    msg.append('location_cookie(%s, %s)'%(pf.location_cookie, fr['location_cookie']))
        p_lc = string.split(pf.location_cookie, '_')[2]
        f_lc = string.split(fr['location_cookie'], '_')[2]
        if p_lc != f_lc:
            msg.append('location_cookie(%s, %s)'%(pf.location_cookie, fr['location_cookie']))
    except (TypeError, ValueError, IndexError, AttributeError):
        msg.append('no or corrupted location_cookie')
        
    # size
    try:
        real_size = os.stat(f)[stat.ST_SIZE]
        if long(pf.size) != long(fr['size']):
            msg.append('size(%d, %d, %d)'%(long(pf.size), long(real_size), long(fr['size'])))
        elif real_size != 1 and long(real_size) != long(pf.size):
            msg.append('size(%d, %d, %d)'%(long(pf.size), long(real_size), long(fr['size'])))
    except (TypeError, ValueError, IndexError, AttributeError):
        msg.append('no or corrupted size')
        
    # file_family
    try:
        if ff.has_key(fr['external_label']):
            file_family = ff[fr['external_label']]
        else:
            vol = infc.inquire_vol(fr['external_label'])
            if vol['status'][0] != e_errors.OK:
                msg.append('missing vol '+fr['external_label'])
                return msg, warn
            file_family = volume_family.extract_file_family(vol['volume_family'])
            ff[fr['external_label']] = file_family
        # take care of MIGRATION, too
        if pf.file_family != file_family and \
            pf.file_family+'-MIGRATION' != file_family:
            msg.append('file_family(%s, %s)'%(pf.file_family, file_family))
    except (TypeError, ValueError, IndexError, AttributeError):
        msg.append('no or corrupted file_family')
        
    # pnfsid
    try:
        pnfs_id = pf.get_pnfs_id()
        if pnfs_id != pf.pnfs_id or pnfs_id != fr['pnfsid']:
            msg.append('pnfsid(%s, %s, %s)'%(pf.pnfs_id, pnfs_id, fr['pnfsid']))
    except (TypeError, ValueError, IndexError, AttributeError):
        msg.append('no or corrupted pnfsid')
        
    # drive
    try:
        if fr.has_key('drive'):	# some do not have this field
            if fr['drive'] != '' and pf.drive != fr['drive']:
                if pf.drive != 'imported' and pf.drive != "missing" \
                    and fr['drive'] != 'unknown:unknown':
                    msg.append('drive(%s, %s)'%(pf.drive, fr['drive']))
    except (TypeError, ValueError, IndexError, AttributeError):
        msg.append('no or corrupted drive')
        
    # path
    try:
        if pf.p_path != fr['pnfs_name0'] and \
               pnfs.get_local_pnfs_path(pf.p_path) != pnfs.get_local_pnfs_path(fr['pnfs_name0']):
            #print layer 4, current name, file database.  ERROR
            msg.append("filename(%s, [%s], %s)" % (pf.p_path, pf.path, fr['pnfs_name0']))
        elif pf.path != pf.p_path and \
                 pnfs.get_local_pnfs_path(pf.path) != pnfs.get_local_pnfs_path(pf.p_path):
            #print current name, then original name.  WARNING
            warn.append('original_pnfs_path(%s, %s)'%(pf.path, pf.p_path))
    except (TypeError, ValueError, IndexError, AttributeError):
        msg.append('no or corrupted pnfs_path')

    # deleted
    try:
        if fr['deleted'] != 'no':
            msg.append('deleted(%s)'%(fr['deleted']))
    except (TypeError, ValueError, IndexError, AttributeError):
        msg.append('no deleted field')

    return msg, warn

def check_file(f):

    #Do one stat() for each file instead of one for each os.path.isxxx() call.
    try:
        f_stats = os.lstat(f)
    except OSError, msg:
	if msg.errno == errno.ENOENT:
            #Before blindly returning this error, first check the directory
            # listing for this entry.  A known 'ghost' file problem exists
            # and this is how to find out.
            directory, filename = os.path.split(f)
            dir_list = os.listdir(directory)
            if filename in dir_list:
                #We have this special error situation.
                error(f + " ... invalid directory entry")
                return
          
            error(f + " ... does not exist")
            return
        if msg.errno == errno.EACCES or msg.errno == errno.EPERM:
            error(f + " ... permission error")
            return
        
        error(f + " ... " + os.strerror(msg.errno))
        return
    
    #if os.path.islink(f):    # skip links
    if stat.S_ISLNK(f_stats[stat.ST_MODE]):    # skip links
        if not os.path.exists(f):
            warning(f+ ' ... missing the original of link')
    # if f is a directory, recursively check its files
    elif stat.S_ISDIR(f_stats[stat.ST_MODE]):
        # skip symbolic link to a directory
        if not stat.S_ISLNK(f_stats[stat.ST_MODE]):
            # skip volmap and .bad and .removed directory
            lc = os.path.split(f)[1]
            if lc != 'volmap' and lc[:4] != '.bad' and lc[:8] != '.removed'\
               and lc[:3] != '.B_' and lc[:3] != '.A_':
                if os.access(f, os.R_OK | os.X_OK):
                    for i in os.listdir(f):
                        check_file(os.path.join(f,i))
                else:
                    error(f+' ... can not access directory')
    elif stat.S_ISREG(f_stats[stat.ST_MODE]):
        print f+' ...',
        res, wrn = check(f)
        # print warnings
        for i in wrn:
            print i+' ...',
        # print errors
        for i in res:
            print i+' ...',
        if res:
            print 'ERROR'
        elif wrn:
            print 'WARNING'
        else:
            print 'OK'
    else:
        error(f+' ... unrecognized type')

if __name__ == '__main__':

    if len(sys.argv) >= 2 and sys.argv[1] == '--help':
        usage()
        sys.exit(0)

    if len(sys.argv) == 3 and sys.argv[1] == '--infile':
        file_object = open(sys.argv[2])
        file_list = None
        #f = open(sys.argv[2])
        #f_list = map(string.strip, f.readlines())
        #f.close()
    elif len(sys.argv) == 1:
        file_object = sys.stdin
        file_list = None
        #f_list = map(string.strip, sys.stdin.readlines())
    else:
        file_object = None
        file_list = sys.argv[1:]
        #f_list = sys.argv[1:]

    intf = option.Interface()
    infc = info_client.infoClient((intf.config_host, intf.config_port))

    #When the entire list of files/directories is listed on the command line
    # we need to loop over them.
    if file_list:
        for line in file_list:
            line = line.strip()
            if line[:2] != '--':
                try:
                    check_file(line)
                except (KeyboardInterrupt, SystemExit):
                    #If the user does Control-C don't traceback.
                    break

    #When the list of files/directories is of an unknown size from a file
    # object; read the filenames in one at a time for resource efficiency.
    elif file_object:
        line = file_object.readline()
        while line:
            line = line.strip()
            try:
                check_file(line)
            except (KeyboardInterrupt, SystemExit):
                #If the user does Control-C don't traceback.
                break
            line = file_object.readline()
