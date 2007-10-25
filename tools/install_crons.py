#!/usr/bin/env python
###############################################################################
#
# $Id$
#
###############################################################################

# system imports
import sys
import os

# enstore modules
import enstore_functions2
import configuration_client
import e_errors

def copy_it(src, dst):
    crontab=os.path.basename(src)

    #Don't clobber a good file.  Make sure that the src is newer 
    # before overwriting.
    if os.path.exists(dst) and os.path.getmtime(src) <= os.path.getmtime(dst):
        sys.stderr.write("%s already exists.\n" % (crontab,) )
        return

    print "Installing crontab:", crontab

    try:
        sf = open(src, "r")
        df = open(dst, "w")

        data = sf.readlines()
        df.writelines(data)
        print "Copied %s to %s." % (src, dst)
        os.system
    except (OSError, IOError), msg:
        sys.stderr.write("%s\n" % (str(msg),))
        return

def delete_it(target):
    crontab=os.path.basename(target)

    if os.path.exists(target):
        print "Uninstalling crontab:", crontab
        os.remove(target)


if __name__ == '__main__':

    #Verify we are on Linux.
    if os.uname()[0] != "Linux":
        sys.stderr.write("Only supported on Linux.\n")
        sys.exit(1)

    #Verify we are user root.
    if os.geteuid() != 0:
        sys.stderr.write("Must be user root.\n")
        sys.exit(1)

    #Verify we have a source directory.
    try:
        CRONJOB_SRC_DIR = os.path.join(os.environ['ENSTORE_DIR'], "crontabs")
        if not os.path.isdir(CRONJOB_SRC_DIR):
            sys.stderr.write("%s does not exist.\n" % (CRONJOB_SRC_DIR,))
            sys.exit(1)
    except KeyError:
        sys.stderr.write("$ENSTORE_DIR not defined.\n")
        sys.exit(1)

    #Verify we have a destination directory.
    CRONJOB_DST_DIR = "/etc/cron.d"
    if not os.path.isdir(CRONJOB_DST_DIR):
        #/etc/cron.d is a Linux specific directory.
        sys.stderr.write("/etc/cron.d does not exist.\n")
        sys.exit(1)

    #Get the cronjob mapping from the configuration server.
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = configuration_client.ConfigurationClient((config_host,config_port))
    res = csc.alive(configuration_client.MY_SERVER, rcv_timeout=10)
    frm_file = 0
    if res['status'][0] != e_errors.OK:
        print "configuration_server %s is not responding ... Get configuration from local file"%(configuration_client.MY_SERVER,)
        csc = configuration_client.configdict_from_file()
        from_file = 1

    cronjobs_dict = csc.get("crontabs")
    if not e_errors.is_ok(cronjobs_dict):
        if from_file == 0:
            sys.stderr.write("Error: %s\n"%(cronjobs_dict.get(['status'],str((e_errors.UNKNOWN, None))),))
            sys.exit(1)

    #Reomve the status from the ticket.
    if cronjobs_dict.has_key('status'): del cronjobs_dict['status']

    for (configuration_key, cron_info) in cronjobs_dict.items():
        use_host = None

        config_info = csc.get(configuration_key)
        if config_info:
            if config_info.has_key('host'):
                use_host = config_info['host']
            elif config_info.has_key('hostip'):
                use_host = config_info['hostip']
        #The first two if/elifs look at the just obtained confg information.
        #  The following elif looks at the host entry in the crontab section
        #  obtained earlier.
        elif cron_info.has_key('host'):
            use_host = cron_info['host']

        if enstore_functions2.is_on_host(use_host) :
            for cron in cron_info['cronfiles']:
                src = os.path.join(CRONJOB_SRC_DIR, cron)
                dst = os.path.join(CRONJOB_DST_DIR, cron)
                copy_it(src, dst)
        else:
            for cron in cron_info['cronfiles']:
                dst = os.path.join(CRONJOB_DST_DIR, cron)
                delete_it(dst)
