#!/usr/bin/env python

import time
import sys
import os
import threading
import subprocess
import socket
import uuid
import random

import ConfigParser


print_lock = threading.Lock()
STOP_FILE="/tmp/STOP"

def print_wrapper(func):
    print_lock.acquire()
    try:
        func
    finally:
        print_lock.release()

def check_stop_file():
    if os.path.exists(STOP_FILE):
        print_message("Found %s file, Stopping ..."%(STOP_FILE,))
        return True
    return False

def log(text):
    sys.stdout.write(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))+" : " +text+"\n")
    sys.stdout.flush()

def e_log(text):
    sys.stderr.write(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))+" : "+text+"\n")
    sys.stderr.flush()

def print_message(txt):
    print_wrapper(log(txt))

def print_error(txt):
    print_wrapper(e_log(txt))

def execute_command(cmd):
    p = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
    output, errors = p.communicate()
    rc=p.returncode
    if rc:
        print_error("Command \"%s\" failed: rc=%d, error=%s"%(cmd,rc,errors.replace('\n',' ')))
        if errors.find("Stale NFS file handle") != -1 :
            print_error("Retrying in 10 seconds");
            time.sleep(10)
            rc=0
        elif errors.find("File exists: Layer 1 and layer 4 are already set") != -1 :
            print_error("Retrying in 10 seconds");
            time.sleep(10)
            rc=0
    return rc

def create_source(name):
    sz=random.gauss(5000.,20.)
    #sz=random.gauss(2.,.3)
    #sz=random.gauss(10.,5.)
    return execute_command("./createfile %f %s"%(sz,name))

def execute(l, func, i, job_config):
    try:
        func(i,job_config)
    finally:
        l.acquire()
        l.notifyAll()
        l.release()

def do_proceed() :
    return not os.path.exists(STOP_FILE)


def main(func,number_of_threads):
    job_config = {}
    cp = ConfigParser.ConfigParser()
    cp.read('lto8.cf')
    path=cp.get('io','pnfs_path','/pnfs/data1/enstore/chimera_test')
    job_config['storage_group']         = cp.get('general','storage_group','none')
    job_config['pnfs_path']             = path
    job_config['number_of_full_passes'] = int(cp.get('full_pass_test','number_of_full_passes',10))
    job_config['number_of_days'] = int(cp.get('random_read_test','number_of_days',20))
    job_config['number_of_mounts']      = int(cp.get('mount_dismount_test','number_of_mounts',8000))
    job_config['read_movers']           = cp.get('random_read_test','read_movers').split(',')
    job_config['mount_movers']          = cp.get('mount_dismount_test','mount_movers').split(',')

    hostname=socket.gethostname().split('.')[0]

    job_config['hostname']=hostname

    print job_config

    lock=threading.Condition(threading.Lock())
    returns={}

    for num in range(number_of_threads):
        t=threading.Thread(target=execute, args=(lock, func,num, job_config),
                           name="Thread-%d"%(num,), kwargs={})
        t.start()

    while True:
        lock.acquire()
        lock.wait(60)
        if  threading.activeCount() <= 1 : break
        lock.release()

    sys.exit(0)

