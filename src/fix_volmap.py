#!/usr/bin/env python

# $Id$
                
            
import sys, os
import string
import pprint
import rexec

_rexec = rexec.RExec()

def eval(stuff):
    return _rexec.r_eval(stuff)

import udp_client
import e_errors
import pnfs


doit = 0
verbose = 0

def get_bfid(name):
    p = os.popen('pnfs cat %s 1'%(name,), 'r')
    ret = p.readlines()
    status = p.close()
    if status:
        raise e_errors.VM_PNFS_EXCEPTION
    return ret[0]

def get_fileinfo(bfid):
    p = os.popen('enstore file --bfid %s '%(bfid,), 'r')
    ret = p.readlines()
    status = p.close()
    if status:
        raise e_errors.VM_ENSTORE_EXCEPTION
    s = string.strip(ret[0])
    return eval(s)

def get_fc_address():
    p = os.popen('conf.sh', 'r')
    lines = p.readlines()
    if p.close():
        raise e_errors.VM_CONF_EXCEPTION
    x = 'file_clerk'
    for line in lines:
        tok = string.split(line,':')
        if tok[0]==x:
            return tok[1], int(tok[2])
    raise e_errors.NO_FC_EXCEPTION
        

def fix_volmap(pnfsname):
    if pnfsname[:5] != '/pnfs':
	a = "%s%s"%(e_errors.NO_PNFS_EXCEPTION, pnfsname)
        raise a

    fc_address = get_fc_address()
    u = udp_client.UDPClient()
    
    bfid = get_bfid(pnfsname)
    fileinfo = get_fileinfo(bfid)

    if verbose:
        pprint.pprint(fileinfo)
    
    keys = ('bfid',
            'complete_crc',
            'deleted',
            'external_label',
            'location_cookie',
            'pnfs_mapname',
            'pnfs_name0',
            'pnfsid',
            'pnfsvid',
            'sanity_cookie',
            'size')

    replaceable = ('deleted', 'pnfs_mapname', 'pnfs_name0', 'pnfsid', 'pnfsvid')

    ok = 1
    for key in keys:
        if not fileinfo.has_key(key) or fileinfo[key] is 'unknown':
            ok = 0
            if key not in replaceable:
                return -1

    if ok:
        return 0

    if not doit:
        return -1
    
    if verbose:    print "Expect an exception..."
    p = pnfs.Pnfs(pnfsname)
    
    p.set_xreference(fileinfo['external_label'],
                     fileinfo['location_cookie'],
                     fileinfo['size'])
    fileinfo['pnfs_name0'] = pnfsname
    fileinfo['pnfsid'] = p.id
    fileinfo["pnfsvid"] = p.volume_fileP.id
    fileinfo["pnfs_mapname"] = p.mapfile

    ticket={'fc':fileinfo, 'work':'set_pnfsid'}

    if verbose: print "sending", pprint.pformat(ticket), "to", fc_address
    
    fc_reply = u.send(ticket, fc_address)
    
    if fc_reply['status'][0] != 'ok':
        print "file clerk error",
        pprint.pprint(fc_reply)
        return -1
    
    if verbose: pprint.pprint(get_fileinfo(bfid))
        
if __name__ == '__main__':
    for arg in sys.argv[1:]:
        if arg[0]=='-':
            if arg[1]=='y':
                doit=1
                continue
            elif arg[1]=='n':
                doit=0
                continue
            elif arg[1]=='v':
                verbose=1
                continue
            else:
                print "Usage %s [-n] [-y] [-v] file..." %(sys.argv[0],)
                sys.exit(-1)
        else:
            try:
                status =  fix_volmap(arg)
                if status:
                    print arg
            except:
                print "ERROR", arg
                

        
        
    
    
    
