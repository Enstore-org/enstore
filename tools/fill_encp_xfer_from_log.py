#!/usr/bin/env python

###############################################################################
#
# $Id$
#
# Purpose of this script is to fill encp_xfer tablle from data in log file
#
###############################################################################

import pg
import time
import sys
import volume_clerk_client
import configuration_client
import enstore_constants
import enstore_functions2

import optparse

def help():
    return "Usage: %prog [options] log file(s) "

if __name__ == "__main__" :
    parser = optparse.OptionParser(usage=help())

    parser.add_option("-s", "--start",
                      metavar="START",type=str,default=None,
                      help="start time in YYYY-MM-DD HH:MM:SS format")


    parser.add_option("-e", "--end",
                      metavar="END",type=str,default=None,
                      help="end time in YYYY-MM-DD HH:MM:SS format")

    (options, args) = parser.parse_args()

    if len(args) < 1:
        parser.print_help()
        sys.exit(1)

    csc=configuration_client.get_config_dict()
    acc = csc.get(enstore_constants.ACCOUNTING_SERVER, None)

    if not acc:
        sys.stderr.write("Failed to extract accounting parameters from configuration\n")
        sys.stderr.flush()
        sys.exit(1)

    db = pg.DB(host   = acc.get("dbhost","localhost"),
               dbname = acc.get("dbname","accounting"),
               port   = acc.get("dbport",8800),
               user   = acc.get("dbuser")) # no default user

    vcc = volume_clerk_client.VolumeClerkClient((enstore_functions2.default_host(),
                                                 enstore_functions2.default_port()),
                                                None,  rcv_timeout=5, rcv_tries=3)

    start_time=options.start
    start=None
    if start_time:
        start = time.mktime(time.strptime(start_time,"%Y-%m-%d %H:%M:%S"))
    stop_time=options.end
    stop=None
    if stop_time:
        stop = time.mktime(time.strptime(stop_time,"%Y-%m-%d %H:%M:%S"))
    #
    # cache volume info
    #
    volumes={}
    version_node_map={}
    for file in args:
        f=open(file,'r')
        date=file[file.find('-')+1:]
        counter = 0
        for line in f:
            if not line : continue
            if line.find('ENCP  Version') != -1:
                parts=line.split()
                node=parts[1].strip()
                version=line[line.find('Version')+len('Version:'):line.find('OS')].strip()
                if not version_node_map.has_key(node):
                    version_node_map[node]=version
            if line.find('MSG_TYPE=ENCP_XFER') == -1: continue
            if counter >= 100: break
            parts = line.split()
            d="%s %s"%(date,parts[0],)
            d_t = time.mktime(time.strptime(d,"%Y-%m-%d %H:%M:%S"))
            if start:
                if d_t<start : continue
            if stop:
                if d_t>stop  : break
            dict = eval(line[line.find('{'):line.find('}')+1])
            dict['date'] = d
            dict['node'] = parts[1]
            dict['username'] = parts[3]
            if parts[7] == 'read_from_hsm':
                dict['rw']='r'
            elif parts[7] == 'write_to_hsm':
                dict['rw']='w'
            else:
                print "what? ", parts[7]
            dict['src']=parts[8]
            dict['dst']=parts[10][:-1]
            if not version_node_map.has_key(dict['node']):
                version="v3_9c  CVS $Revision$ <frozen>"
                if dict['node'].find('cmsstor') != -1:
                    dict['encp_version']="v3_7d  CVS $Revision$ encp"
                dict['encp_version']=version
                version_node_map[dict['node']]=version
            else:
                dict['encp_version']=version_node_map[dict['node']]
            for p in parts[10:]:
                if p.find('=') == -1 : continue
                (p1,p2) = p.split('=')
                dict[p1]=p2
            dict['pid']=dict['unique_id'].split('-')[2]
            dict['encp_id']=dict['unique_id']
            dict['size']=parts[11]
            dict['volume']=parts[15]
            del dict['encp_crc']
            del dict['drive_vendor']
            del dict['unique_id']
            if not volumes.has_key(dict['volume']) :
                volumes[dict['volume']]=vcc.inquire_vol(dict['volume'])
            vol_info=volumes.get(dict['volume'])
            dict['wrapper']=vol_info['wrapper']
            dict['file_family']=vol_info['volume_family'].split('.')[2]
            dict['library'] = vol_info['library']
            #counter = counter + 1
            q="select * from encp_xfer where node='%s' and pid=%s and date='%s' and volume='%s'"%(dict['node'],dict['pid'],dict['date'],dict['volume'],)
            res=db.query(q)
            if res.ntuples():
                print "Skipping.... ",  res.ntuples() , dict['date']
            else:
                print "Inserting.... ",  res.ntuples() , dict['date']
                r=db.insert('encp_xfer',dict)
        f.close()
    db.close()
    sys.exit(0)
