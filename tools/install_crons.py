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
    #Don't a good file.  Make sure that the src is newer before overwriting.
    if os.path.exists(dst) and os.path.getmtime(src) <= os.path.getmtime(dst):
        sys.stderr.write("%s already exists.\n")
        return

    try:
        sf = open(src, "r")
        df = open(dst, "w")

        data = sf.readlines()
        df.writelines(data)
        print "Copied %s to %s." % (src, dst)
    except (OSError, IOError), msg:
        sys.stderr.write("%s\n" % (str(msg),))
        return

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
    cronjobs_dict = csc.get("crontabs")
    if not e_errors.is_ok(cronjobs_dict):
        sys.stderr.write("Error: %s\n" %
                         cronjobs_dict.get(['status'],
                                           str((e_errors.UNKNOWN, None))))
        sys.exit(1)

    #Reomve the status from the ticket.
    del cronjobs_dict['status']

    for (configuration_key, cron_info) in cronjobs_dict.items():
        use_host = None

        config_info = csc.get(configuration_key)
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
                if not os.path.exists(dst):
                    print "Installing crontab:", cron
                    copy_it(src, dst)
        else:
            for cron in cron_info['cronfiles']:
                dst = os.path.join(CRONJOB_DST_DIR, cron)
                if os.path.exists(dst):
                    print "Uninstalling crontab:", cron
                    os.remove(dst)
