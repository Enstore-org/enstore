#!/usr/bin/env python

# $Id$
import os
import sg_db
import volume_clerk_client
import volume_family
import configuration_client

class OpSGDB:
    def __init__(self):
        config_host = os.environ.get('ENSTORE_CONFIG_HOST')
        config_port = int(os.environ.get('ENSTORE_CONFIG_PORT'))
        self.vcc = volume_clerk_client.VolumeClerkClient((config_host, config_port))
        dbInfo = configuration_client.ConfigurationClient((config_host, config_port)).get('database')
        self.dbHome = dbInfo['db_dir']
        # get all volumes know to the volume clerk
        self.volumes = self.vcc.get_vols()['volumes'] 

    # create a dictionary of libraries-stograge groups for all volumes
    def create_sg_dict(self):
        sgs = {}
        for vol in self.volumes:
            lib = vol['library']
            sg = volume_family.extract_storage_group(vol['volume_family'])
            if sg != 'none':
                sg_count = sgs.get((lib, sg), 0)
                sgs[(lib,sg)] = sg_count+1
        return sgs

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

def test():
    opdb = OpSGDB()
    print "GET ALL VOLUMES"
    print opdb.volumes
    print "classify by library-storage group"
    sg_dict = opdb.create_sg_dict()
    print sg_dict
    print "create DB"
    opdb.create_db()
    print "make dictionary"
    print opdb.make_dict()


if __name__=='__main__':
    test()
