#!/usr/bin/env python

import os
import sys
import string

import e_errors
import timeofday

import configuration_client
import log_client
import alarm_client
import inquisitor_client
import file_clerk_client
import volume_clerk_client
import library_manager_client
import media_changer_client
import mover_client

config_port = string.atoi(os.environ.get('ENSTORE_CONFIG_PORT', 7500))
config_host = os.environ.get('ENSTORE_CONFIG_HOST', "localhost")
config=(config_host,config_port)

timeout=15
tries=1

print timeofday.tod(), 'Checking Enstore on',config,'with timeout of',timeout,'and tries of',tries

def down():
    print timeofday.tod(),'Finished checking Enstore... system is defined to be DOWN'
    sys.exit(1)

def up():
    print timeofday.tod(), 'Finished checking enstore ... system is defined to be UP'
    sys.exit(0)


def check_ticket(server, ticket,exit_if_bad=1):
    if not 'status' in ticket.keys():
        print timeofday.tod(),server,' NOT RESPONDING'
        if exit_if_bad:
            down()
        else:
            return 1
    if ticket['status'][0] == e_errors.OK:
        print timeofday.tod(), server, ' ok'
        return 0
    else:
        print timeofday.tod(), server, ' BAD STATUS',ticket['status']
        if exit_if_bad:
            down()
        else:
            return 1

def sortit(adict):
    alist = []
    for key in adict.keys():
        alist.append(key)
    alist.sort()
    return alist

csc = configuration_client.ConfigurationClient(config)
check_ticket('Configuration Server',csc.alive(timeout,tries))

lcc = log_client.LoggerClient(csc, "LOG_CLIENT", "log_server")
check_ticket('Logger',lcc.alive('log_server',timeout,tries))

acc = alarm_client.AlarmClient(csc, timeout,tries)
check_ticket('Alarm Server',acc.alive('alarm_server',timeout,tries))

ic = inquisitor_client.Inquisitor(csc)
check_ticket('Inquisitor',ic.alive('inquisitor',timeout,tries),0)

fcc = file_clerk_client.FileClient(csc, 0)
check_ticket('File Clerk',fcc.alive('file_clerk',timeout,tries))

vcc = volume_clerk_client.VolumeClerkClient(csc)
check_ticket('Volume Clerk',vcc.alive('volume_clerk',timeout,tries))

library_managers = sortit(csc.get_library_managers({}))

meds = {}

for lm in library_managers:
    lmc = library_manager_client.LibraryManagerClient(csc,lm)
    check_ticket(lm+' Library Manager', lmc.alive(lm+'.library_manager',timeout,tries))
    meds[csc.get_media_changer(lm+'.library_manager',timeout,tries)] = 1 # no duplicates in dictionary
    movs = {}
    mov=csc.get_movers(lm+'.library_manager')
    for m in mov:
        movs[(m['mover'])] = 1 # no duplicates in dictionary
    movers = sortit(movs)
    num_movers = 0
    bad_movers = 0
    for mov in movers:
        num_movers=num_movers+1
        mvc = mover_client.MoverClient(csc,mov)
        bad_movers = bad_movers + check_ticket(mov+' Mover',mvc.alive(mov,timeout,tries),0)
    if bad_movers*2 > num_movers:
        print timeofday.tod(), 'LOW CAPACITY: Found', bad_movers, 'of', num_movers, 'not responding'
        down()
    else:
        print timeofday.tod(), 'Sufficient capacity of movers for',lm, num_movers-bad_movers, 'of', num_movers, 'responding'

media_changers = sortit(meds)

for med in media_changers:
    mcc = media_changer_client.MediaChangerClient(csc,med)
    check_ticket(med+' Media Changer',mcc.alive(med,timeout,tries))

up()
