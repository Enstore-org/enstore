#!/usr/bin/env python

from utils import *
import time
import sys

def write(i,job_config):
    source="dummy_%d"%(i,)
    if not os.path.exists(source):
        rc=create_source(source)
        if rc:
            return rc
    pnfs_path=job_config.get('pnfs_path')
    hostname=job_config.get('hostname')
    destination=os.path.join(pnfs_path,hostname,"%d"%(i,))
    if not os.path.exists(destination):
        os.makedirs(destination)
    count=0
    while True:
        if os.path.exists(STOP_FILE): break
        output=os.path.join(destination,"%s_%s.data"%(hostname,uuid.uuid1().hex))
        if os.path.exists(output) :
            continue
        #cmd="/opt/enstore_cache/bin/encp --enable-redirection %s %s"%(source,output,)
        cmd="encp --enable-redirection %s %s"%(source,output,)
        #cmd="encp --verbose 11 --enable-redirection %s %s"%(source,output,)
        rc=execute_command(cmd)
        if rc:
            os.system("touch %s"%(STOP_FILE))
            return rc
        count = count + 1
        if count % 10 == 0 :
            print_message("done %d transfers "%(count,))
        time.sleep(20)
    return 0

if __name__ == "__main__":
    main(write,number_of_threads=int(sys.argv[1]))
