#!/usr/bin/env python

# $Id$
import os
import sg_db
import volume_clerk_client
import volume_family
import configuration_client
import e_errors

KB=1024.
MB=KB*KB
GB=KB*MB
TB=MB*MB
#turn byte count into a nicely formatted string
def capacity_str(x,mode="GB"):
    if mode == "GB":
        z = x/1024./1024./1024. # GB
        return "%7.2fGB"%(z,)

    x=1.0*x    ## make x floating-point
    neg=x<0    ## remember the sign of x
    x=abs(x)   ##  make x positive so that "<" comparisons work
        
    for suffix in ('B ', 'KB', 'MB', 'GB', 'TB', 'PB'):
        if x <= 1024:
            break
        x=x/1024
    if neg:    ## if x was negative coming in, restore the - sign  
        x = -x
    return "%6.2f%s"%(x,suffix)

class OpSGDB:

    def __init__(self):
        config_host = os.environ.get('ENSTORE_CONFIG_HOST')
        config_port = int(os.environ.get('ENSTORE_CONFIG_PORT'))
        self.vcc = volume_clerk_client.VolumeClerkClient((config_host, config_port))
        dbInfo = configuration_client.ConfigurationClient((config_host, config_port)).get('database')
        self.dbHome = dbInfo['db_dir']
        # get quotas
        self.quotas = configuration_client.ConfigurationClient((config_host, config_port)).get('quotas')
        # get all volumes know to the volume clerk
        self.volumes = self.vcc.get_vols(print_list=0)['volumes']



    # create a dictionary of libraries-stograge groups for all volumes
    def create_sg_dict(self):
        sgs = {}
        vols = []
        for vol in self.volumes:
            vols.append(vol['volume'])
            lib = vol['library']
            sg = volume_family.extract_storage_group(vol['volume_family'])
            ff = volume_family.extract_file_family(vol['volume_family'])
            if not sgs.has_key((lib,sg)) and sg != 'none':
                sgs[(lib,sg)] = {}
                sgs[(lib,sg)]['blank'] = 0
                sgs[(lib,sg)]['used'] = 0
                sgs[(lib,sg)]['total'] = 0.
                sgs[(lib,sg)]['quota'] = self.quotas['libraries'][lib].get(sg, '?')
                sgs[(lib,sg)]['deleted'] = []
                    
            if sg != 'none':
                if ff == 'none' or (vol['sum_wr_access'] == 0 and vol['system_inhibit'][0] != e_errors.DELETED):  # blank volume
                    sgs[(lib,sg)]['blank'] = sgs[(lib,sg)]['blank']+1
                if vol['system_inhibit'][0] == e_errors.DELETED:
                    if vol['volume'] not in sgs[(lib,sg)]['deleted']:
                        # count the same volume onlu once
                        sgs[(lib,sg)]['deleted'].append(vol['volume']) 
                if vol['sum_wr_access'] != 0:
                    sgs[(lib,sg)]['used'] = sgs[(lib,sg)]['used']+1
                    sgs[(lib,sg)]['total'] = sgs[(lib,sg)]['total'] + vol['capacity_bytes']*1. - vol['remaining_bytes']*1.
        return sgs

    def print_sg_dict(self, sgs):
        keys=sgs.keys()
        keys.sort()
        print "%-10s %-20s %-6s %-10s %-012s %-07s %-012s" % ('Library', 'Storage Group', 'Quota',
                                                       'Blank Vols', 'Written Vols', 'Deleted', 'Space Used')
        for key in keys:
            print "%-10s %-20s %-6s %-10s %-14s %-7s %-012s" % (key[0], key[1], sgs[key]['quota'],
                                                                sgs[key]['blank'], sgs[(key)]['used'],
                                                                len(sgs[key]['deleted']),
                                                                capacity_str(sgs[key]['total']))

    # create database entries
    def create_db(self):
        sgs = self.create_sg_dict()
        db = sg_db.SGDb(self.dbHome)
        for entry in sgs.keys():
            db.delete_sg_counter(entry[0], entry[1],force=1)
            db.inc_sg_counter(entry[0], entry[1], sgs[entry])
        
    # generate a dictionary in the configuration file format
    def make_dict(self, sg_dict=None):
        quotas = {}
        sgs = self.create_sg_dict()
        db = sg_db.SGDb(self.dbHome)
        for entry in sgs.keys():
            if not quotas.has_key(entry[0]):
                quotas[entry[0]] = {}
            if sg_dict:
                count = sg_dict[entry]
            else:
                count = db.get_sg_counter(entry[0], entry[1])
            quotas[entry[0]][entry[1]] = count
        return quotas

def report():
    opdb = OpSGDB()
    #classify by library-storage group
    sg_dict = opdb.create_sg_dict()
    opdb.print_sg_dict(sg_dict)


if __name__=='__main__':
    report()
