#!/usr/bin/env python

import os
import pprint
import string

import configuration_client
import timeofday

if __name__=="__main__":

    drives = {}
    command = "dasadmin listd2"
    listd2 = os.popen(command,'r').readlines()
    for line in range(0,len(listd2)):
        #print listd2[line]
        tokens = string.split(listd2[line])
        if len(tokens)>0 and tokens[0]=="drive:":
            #print tokens[1],len(tokens),tokens[6]
            drives[tokens[1]] = {'state':tokens[6]}
        elif tokens[0]=="listd2":
            pass
        else:
            print "Can not parse line", listd2[line]
    #pprint.pprint(drives)

    csc = configuration_client.ConfigurationClient(('d0ensrv2',7500))
    ckeys = csc.get_keys()
    #pprint.pprint(ckeys)
    movers=[]
    for item in ckeys['get_keys']:
        if string.find(item,'.mover')!=-1:
            movers.append(item)
    #pprint.pprint(movers)
    cdump = csc.dump()
    #pprint.pprint(cdump)

    for drive in drives.keys():
        dinfo = drives[drive]
        dinfo['name'] = 'UNKNOWN'
        dinfo['config'] = 'UNKNOWN'
        dinfo['host'] = 'UNKNOWN'
        dinfo['library'] = 'UNKNOWN'
        dinfo['device']  = 'UNKNOWN'
        for mover in movers:
            if string.find(mover,drive)!=-1:
                dinfo['name'] = mover
                dinfo['config'] = cdump['dump'][mover]
                dinfo['host'] = cdump['dump'][mover].get('host','UNKNOWN')
                dinfo['library'] = cdump['dump'][mover].get('library',['UNKNOWN',])[0]
                dinfo['device']  = cdump['dump'][mover].get('device','UNKNOWN')
                drives[drive] = dinfo
                break
    #pprint.pprint(drives)

    down = []
    unattached = []
    sammam = []
    samm2 = []
    testa2 = []
    testm2 = []
    testlto = []
    other = []

    for drive in drives.keys():
        dinfo = drives[drive]
        if dinfo['state'] == 'DOWN':
            down.append(drive)
        elif dinfo['library'] == 'sammam.library_manager':
            sammam.append(drive)
        elif dinfo['library'] == 'samm2.library_manager':
            samm2.append(drive)
        elif dinfo['library'] == 'testa2.library_manager':
            testa2.append(drive)
        elif dinfo['library'] == 'testm2.library_manager':
            testm2.append(drive)
        elif dinfo['library'] == 'testlto.library_manager':
            testlto.append(drive)
        elif dinfo['name'] == 'UNKNOWN':
            unattached.append(drive)
        else:
            other.append(drive)

    down.sort()
    unattached.sort()
    sammam.sort()
    samm2.sort()
    testa2.sort()
    testm2.sort()
    testlto.sort()
    other.sort()

    print 'D0 AML/2 - Enstore Drive Report'
    print timeofday.tod()
    print "\nFound",len(drives),"defined in the AML/2"
    print "\nFound",len(sammam),"drives in AML/2 in sammam library"
    print "\nFound",len(testa2),"drives in AML/2 in samm2 library"
    print "\nFound",len(down),"drives marked DOWN in AML/2"
    if len(down)!=0:
        print "\t",down
        print "\t Please verify that this is correct"
    print "\nFound",len(testa2),"drives in AML/2 in testa2 library"
    if len(testa2)!=0:
        print "\t",testa2
        print "\t Please verify that this is correct"
    print "\nFound",len(testm2),"drives in AML/2 in testm2 library"
    if len(testm2)!=0:
        print "\t",testm2
        print "\t Please verify that this is correct"
    print "\nFound",len(testlto),"drives in AML/2 in testlto library"
    if len(testlto)!=0:
        print "\t",testlto
        print "\t Please verify that this is correct"
    print "\nFound",len(unattached),"drives in AML/2, but not defined to any enstore mover"
    if len(unattached)!=0:
        print "\t",unattached
        print "\tThis can not be correct"
        print "\tEither mark drives as DOWN in AML/2 or attached them to a mover or remove them from AML/2"
    print "\nFound",len(other),"drives in non-production libraries"
    for drive in other:
        dinfo = drives[drive]
        print drive, dinfo['name'],dinfo['host'],dinfo['library'],dinfo['device']
    if len(other)!=0:
        print "THE MAXIMUM SUGGESTED TIME A DRIVE SHOULD BE IN a NON-PRODUCTION LIBRARY IS 1 DAY."
