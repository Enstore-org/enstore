#!/usr/bin/env python

# $Id$

#system imports
import sys,os
import string
import getopt
import socket
import pprint

#XXX cgw hack
sys.path.insert(0,"../src")

#enstore imports
import configuration_client
import file_clerk_client
import volume_clerk_client
import e_errors
import pnfs

def read_input_file(fname):
    volumes = {}
    infile = open(fname,'r')
    for line in infile.readlines():
        line = string.strip(line)
        key, value = string.split(line)
        words = string.split(key,'/')
        if not words:
            continue

        if len(words)==5 and words[0]=="volumes" and words[2]=="files":
            volname, file, key = words[1], words[3], words[4]
        elif len(words)==3 and words[0]=="volumes":
            volname, file, key = words[1], None, words[2]
        else:
            print "Invalid input line", line
            sys.exit(-1)
        if volname not in volumes.keys():
            volumes[volname] = {}
        vdict = volumes[volname]
        if file:
            if 'files' not in vdict.keys():
                vdict['files'] = {}
            if file not in vdict['files'].keys():
                vdict['files'][file] = {}
            vdict['files'][file][key] = value
        else:
            vdict[key] = value
    return volumes


def mkdir_p(path): # python 1.5.2 has makedirs but we may not be running that version

    if os.path.exists(path):
        if not os.path.isdir(path):
            raise "OSError", (path, "not a directory")
        else:
            return

    head, tail = os.path.split(path)

    if not head:
        os.mkdir(tail, 0777)
        return
    
    if os.path.exists(head):
        if not os.path.isdir(head):
            raise "OSError", (head, "not a directory")
        os.mkdir(path, 0777)
    else:
        mkdir_p(head)
        if tail:
            os.mkdir(path, 0777)
        
    
config_host = os.environ.get("ENSTORE_CONFIG_HOST", "localhost")
config_port = string.atoi(os.environ.get("ENSTORE_CONFIG_PORT", "7500"))


if __name__=="__main__":
    longopts =  ["config-host=", "config-port=","verbose", "media-type="]
    verbose=0
    arglist = []
    ##be friendly about '_' vs '-'
    for arg in sys.argv[1:]:
        arglist.append(string.replace(arg,'_','-'))
    opts, args = getopt.getopt(arglist, "", longopts)
    media_type = None
    for opt,val in opts:
        if opt=="--config-host":
            config_host=val
        elif opt=="--config-port":
            config_port=string.atoi(val)
        elif opt=="--verbose":
            verbose=1
        elif opt=="--media-type":
            media_type=val
        
    if len(args) != 1:
        print "Usage:\n", sys.argv[0],
        for optname in longopts:
            if optname[-1]=='=': print "--"+optname+"value",
            else: print "--"+optname,
        print "volume_description_file"
        sys.exit(-1)

    if media_type is None:
        print "Media type must be specified"
        sys.exit(-1)
        
    volume_dict = read_input_file(args[0])
    
    if config_host=="localhost":
        config_host = socket.gethostname()
        
    csc = configuration_client.ConfigurationClient((config_host,config_port))

    file_clerk_ticket = csc.get("file_clerk")
    if file_clerk_ticket['status'][0] != e_errors.OK:
        print "Cannot talk to file clerk", file_clerk_ticket['status']
        sys.exit(-1)
    file_clerk_address = (file_clerk_ticket['hostip'],
                              file_clerk_ticket['port'])
    fcc = file_clerk_client.FileClient(csc, 0, 
                                       file_clerk_address)

    volume_clerk_ticket = csc.get("volume_clerk")
    volume_clerk_address = (volume_clerk_ticket['hostip'],
                            volume_clerk_ticket['port'])
    vcc = volume_clerk_client.VolumeClerkClient(csc, 
                                                volume_clerk_address)

    
    for vol_name in volume_dict.keys():
        vol = volume_dict[vol_name]
        library = "shelf"
        file_family = vol.get("hostname", "import.unknown") #+".cpio_odc"
        #media_type came from cmdline
        capacity_bytes=0
        remaining_bytes=0
        nfiles = string.atoi(vol["next_file"]) - 1
        eod_cookie = "0000_000000000_%07d"%(nfiles+1) #+1 to skip VOL1 label
        user_inhibit = "none"
        error_inhibit = "none"
        first_access = string.atof(vol["first_access"])
        if vol.has_key("last_access"):
            last_access = string.atof(vol["last_access"])
        else:
            last_access = first_access
        declared = first_access
        sum_wr_err = 0
        sum_rd_err = 0
        non_del_files = sum_wr_access = nfiles
        sum_rd_access = 0
        wrapper = vol["format"]
        blocksize = string.atoi(vol["blocksize"])
        system_inhibit = "readonly"
        
        if verbose: print "addvol", vol_name
        ## addvol, set file family to remote hostname (from metadata)
        done_ticket = vcc.add( library,      
                               file_family,       
                               media_type,        
                               vol_name,
                               capacity_bytes,    
                               remaining_bytes,   
                               eod_cookie,
                               user_inhibit,
                               error_inhibit,
                               last_access,
                               first_access,
                               declared,
                               sum_wr_err,
                               sum_rd_err,
                               sum_wr_access,
                               sum_rd_access,
                               wrapper,
                               blocksize,
                               non_del_files,
                               system_inhibit)

        status = done_ticket["status"]
        if status[0] != "ok":
            print status
            sys.exit(-1)
        
        
        for file_num in vol['files'].keys():
            file = vol['files'][file_num]
            n = string.atoi(file_num) 
            loc_cookie = "0000_000000000_%07d" % n
            if (file['early_checksum_size'] == 'None'
                or file['early_checksum'] == 'None'):
                sanity_cookie = 0, None
            else:
                sanity_cookie = (string.atoi(file['early_checksum_size']),
                                 string.atol(file['early_checksum']))
            size = string.atoi(file['size'])
            if file['checksum'] == 'None':
                complete_crc = None
            else:
                complete_crc = string.atol(file['checksum'])
            ticket = {
                "work":"new_bit_file",
                "fc": {"external_label": vol_name,
                       "location_cookie": loc_cookie,
                       "size": size,
                       "sanity_cookie": sanity_cookie,
                       "complete_crc": complete_crc
                       }
                }

            # create a new bit file id
            if verbose: print "add_file", file
            done_ticket = fcc.new_bit_file(ticket)

            status = done_ticket["status"]
            if status[0] != "ok":
                print status
                sys.exit(-1)
            

            pnfs_filename = file['destination']

            ## create the base directories
            pnfs_dir = os.path.dirname(pnfs_filename)
            if verbose: print "creating directory", pnfs_dir
            mkdir_p(pnfs_dir)

            # create PNFS cross-reference
            p = pnfs.Pnfs(pnfs_filename)
            p.set_bit_file_id(done_ticket["fc"]["bfid"], size)
            
            if verbose:
                print vol_name,loc_cookie,size
            
            # create volume map and store cross reference data
            p.set_xreference(vol_name, loc_cookie, size)
            
            ticket["work"] = "set_pnfsid"
            ticket["fc"].update({
                "pnfsvid":p.volume_fileP.id,
                "pnfs_name0": p.pnfsFilename,
                "pnfs_mapname": p.volume_fileP.pnfsFilename,
                "pnfsid": p.id, 
                "bfid": done_ticket['fc']['bfid']
                })

            if verbose: print "setting pnfsid"
            done_ticket = fcc.set_pnfsid(ticket)
           
            status = done_ticket["status"]
            if status[0] != "ok":
                print status
                sys.exit(-1)
            
 
            
            
            
            
