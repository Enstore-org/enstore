#!/usr/bin/env python

import time
import sys
import os
import threading
import subprocess
import socket
import uuid
import random
import pnfs

import ConfigParser

import configuration_client
import enstore_functions2

print_lock = threading.Lock()
STOP_FILE="/tmp/STOP"

def print_wrapper(func):
    print_lock.acquire()
    try:
        func
    finally:
        print_lock.release()

def log(text):
    sys.stdout.write(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))+" : " +threading.current_thread().getName()+" : " +text+"\n")
    sys.stdout.flush()

def e_log(text):
    sys.stderr.write(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))+" : " +threading.current_thread().getName()+" : "+text+"\n")
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
    return rc

def create_source(name):
    sz=random.gauss(5120.,1024.)
    return execute_command("./createfile %f %s"%(sz,name))

def set_tags(dirname,
             library,
             file_family,
             file_family_width=1):
    p = pnfs.Tag(dirname)
    p.set_library(library, dirname)
    p.set_file_family(file_family, dirname)
    p.set_file_family_width(file_family_width, dirname)

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
    cp.read('lto5.cf')
    path=cp.get('io','pnfs_path','/pnfs/data1/test/litvinse/NULL')
    job_config['pnfs_path']             = path
    job_config['number_of_full_passes'] = int(cp.get('full_pass_test','number_of_full_passes',10))
    job_config['number_of_read_passes'] = int(cp.get('random_read_test','number_of_read_passes',4))
    job_config['number_of_mounts']      = int(cp.get('mount_dismount_test','number_of_mounts',8000))
    job_config['read_movers']           = cp.get('random_read_test','read_movers').split(',')
    job_config['mount_movers']          = cp.get('mount_dismount_test','mount_movers').split(',')

    hostname=socket.gethostname().split('.')[0]
    csc   = configuration_client.ConfigurationClient((enstore_functions2.default_host(),
                                                      enstore_functions2.default_port()))
    #
    # find library running on this host
    #
    lms=csc.get_library_managers()
    library=None
    library_manager=None
    for name, lm in lms.iteritems():
        lm_host = lm.get('address')[0].split('.')[0]
        if lm_host == hostname and name != 'LTO3' and name != 'null2' :
            library = name
            library_manager = lm
            break

    if not library:
        print_error("LM is not running on host %s. Quitting."%(hostname))
        sys.exit(1)

    if os.path.exists(STOP_FILE):
        os.unlink(STOP_FILE)

    job_config['library']=library
    job_config['hostname']=hostname
    job_config['database'] =csc.get("database", {})

    #
    # find mover running on this host
    #
    mover_list=csc.get_movers(library_manager.get('name'))
    if len(mover_list) == 0 :
        print_error("No movers associated with %s"%(library_manager.get('name')))
        sys.exit(1)
    mover = None
    for m in mover_list:
        m_host = socket.gethostbyaddr(m.get('address')[0])[0].split('.')[0]
        if m_host == hostname :
            mover = m.get('mover')
            break
    if not mover :
         print_error("No movers associated with %s on this host %s"%(library_manager.get('name'),hostname))
         sys.exit(1)
    #
    # get info about the mover
    #
    mover_info = csc.get(mover)

    job_config['mover']=mover_info

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

