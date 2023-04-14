###############################################################################
#
# $Id$
#
###############################################################################

import pg
import time
import sys

import media_changer_client
import configuration_client
import enstore_constants
import enstore_functions2
import e_errors

def update_slots(csc, mcc):
    slots_list = []
    now = time.time()
    #First get the name of the tape library.
    tape_library = csc.get(mcc.server_name, 5, 5).get('tape_library',
                                                           None)
    if tape_library == None:
        return

    slots_dict = mcc.list_slots(rcv_timeout = 100, rcv_tries = 3)

    if e_errors.is_ok(slots_dict):
        slots_list = slots_dict['slot_list']
    else:
        sys.stderr.write("%s: Failed to get slots list from media changer: %s\n" %
                             (time.ctime(), slots_dict['status']))
        return
    acc_db = None
    try:
        ## Put the information into the accounting DB.
        acc_conf = csc.get(enstore_constants.ACCOUNTING_SERVER,None)
        if not acc_conf:
            sys.stderr.write("%s: Can not find accounting in configuration"%     (time.ctime()))
            return
        acc_db   = pg.DB(host  = acc_conf.get('dbhost', "localhost"),
                         port  = acc_conf.get('dbport', 5432),
                         dbname= acc_conf.get('dbname', "accounting"),
                         user  = acc_conf.get('dbuser', "enstore"))
        for slot_info in slots_list:
            q="insert into tape_library_slots_usage (time, tape_library, \
            location, media_type, total, free, used, disabled) values \
            ('%s', '%s', '%s', '%s', %d, %d, %d, %d)" % \
                (time.strftime("%m-%d-%Y %H:%M:%S %Z", time.localtime(now)),
                 tape_library,
                 slot_info['location'],
                 slot_info['media_type'],
                 slot_info['total'],
                 slot_info['free'],
                 slot_info['used'],
                 slot_info['disabled'])
            acc_db.query(q)
        acc_db.close()
        return
    except:
        exc, msg, tb = sys.exc_info()
        try:
            sys.stderr.write("%s: Can not update DB: (%s, %s)\n" %
                             (time.ctime(), exc, msg))
            sys.stderr.flush()
        except IOError:
            pass
        if acc_db:
            acc_db.close()

if __name__ == "__main__":
    csc = configuration_client.ConfigurationClient((enstore_functions2.default_host(),
                                                    enstore_functions2.default_port()))
    mcs = csc.get_media_changers2(timeout = 3, retry = 10)
    for mc in mcs:
        mcc = media_changer_client.MediaChangerClient(
            csc, name = mc['name'],
            rcv_timeout = 3, rcv_tries = 3
            )
        update_slots(csc,mcc)


