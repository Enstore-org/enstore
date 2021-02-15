from __future__ import print_function
import os
import sys

import library_manager_client
import configuration_client
import volume_family
import log_client
import enstore_functions2
import option
import Trace
import e_errors

MY_NAME = "SET_LM_NOREAD"
WRITE_LIMIT = 20
PANIC_WRITE_LIMIT = 50
READ_LIMIT = 0
NOREAD = 'noread'
UNLOCKED = 'unlocked'

"""
check a library manager to see if it has many read queues up for the entered file families.
if so, and there are read jobs for these file families, set the lm to noread so that the
writes can finish.  set the lm back to unlocked if the writes are done.

"""


def get_ff(vc):
    ff = vc.get('file_family', None)
    if not ff:
        vf = vc.get('volume_family', None)
        if vf:
            ff = volume_family.extract_file_family(vf)
    return ff


def parse_queue(queue, ff_d, ff_k):
    for elem in queue:
        ff = get_ff(elem['vc'])
        if ff in ff_k:
            ff_d[ff] = ff_d[ff] + 1


Trace.init(MY_NAME)
config_host = enstore_functions2.default_host()
config_port = enstore_functions2.default_port()

if config_host and config_port:
    logc = log_client.LoggerClient((config_host, config_port), MY_NAME)
    if len(sys.argv) < 3:
        # there was no lib man or file family entered, exit
        msg = "No library manager specified or file family specified"
        Trace.log(e_errors.WARNING, msg)
        print(msg)
    else:
        csc = configuration_client.ConfigurationClient((config_host,
                                                        config_port))
        lm = sys.argv[1]
        ff_l = []
        ff_d = {}
        ff_d_r = {}
        # get a list of all the file families to look for
        for arg in sys.argv[2:]:
            ff_d[arg] = 0
            ff_d_r[arg] = 0

        ff_k = ff_d.keys()
        lmc = library_manager_client.LibraryManagerClient(csc, lm)
        ticket = lmc.get_lm_state()
        if e_errors.is_ok(ticket):
            state = ticket['state']
            ticket = lmc.getworks_sorted()
            if e_errors.is_ok(ticket):
                pq = ticket['pending_works']
                adminq = pq['admin_queue']
                writeq = pq['write_queue']
                readq = pq['read_queue']

                parse_queue(adminq, ff_d, ff_k)
                parse_queue(writeq, ff_d, ff_k)
                parse_queue(readq, ff_d_r, ff_k)

                for ff in ff_k:
                    print(ff, ff_d[ff], ff_d_r[ff])
                    if (ff_d[ff] > WRITE_LIMIT and ff_d_r[ff] > READ_LIMIT) or \
                       ff_d[ff] > PANIC_WRITE_LIMIT:
                        if not state == NOREAD:
                            msg = "Setting %s to %s, %s has %s writes and %s reads" % (lm,
                                                                                       NOREAD, ff, ff_d[ff], ff_d_r[ff])
                            Trace.log(e_errors.WARNING, msg)
                            print(msg)
                            lmc.change_lm_state(NOREAD)
                        # we do not have to check any more
                        break
                else:
                    # we did not change the state of the lm to noread.
                    # see if it is in noread, if so set it back to normal
                    if state == NOREAD:
                        msg = "Setting %s to %s" % (lm, UNLOCKED)
                        Trace.log(e_errors.WARNING, msg)
                        print(msg)
                        ticket = lmc.change_lm_state(UNLOCKED)
