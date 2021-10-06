#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# system import
import os
import sys
import socket
import select
import string
import time
import threading
import types
import pg

# enstore import
#import configuration_client
import dispatching_worker
import generic_server
import volume_clerk_client
#import configuration_client
import timeofday
#import udp_client
#import enstore_functions2
import enstore_constants
#import monitored_server
#import event_relay_client
import event_relay_messages
import Trace
import e_errors
import media_changer_client
import volume_family
import mover_client


MY_NAME = enstore_constants.RATEKEEPER    #"ratekeeper"

rate_lock = threading.Lock()
acc_db_lock = threading.Lock()

CHILD_TTL = 300

#Note: These intervals should probably come from the configuration file.
DRVBUSY_INTERVAL = 900 #15 minutes
SLOTS_INTERVAL = 21600 #6 hours
MVR_RETRY_INTERVAL = 10
MVR_RETRIES=1

def endswith(s1,s2):
    return s1[-len(s2):] == s2

#def atol(s):
#    if s[-1] == 'L':
#        s = s[:-1] #chop off any trailing "L"
#    return string.atol(s)

def next_minute(t=None):
    if t is None:
        t = time.time()
    Y, M, D, h, m, s, wd, jd, dst = time.localtime(t)
    m = (m+1)%60
    if m==0:
        h=(h+1)%24
        if h==0:
            D=D+1
            wd=wd+1
            jd=jd+1
            ##I'm not going to worry about end-of-month.  Sue me!
    t = time.mktime((Y, M, D, h, m, 0, wd, jd, dst))
    return t

class Ratekeeper(dispatching_worker.DispatchingWorker,
                 generic_server.GenericServer):

    interval = 15
    resubscribe_interval = 10*60
    def __init__(self, csc):
        generic_server.GenericServer.__init__(self, csc, MY_NAME,
                                              #function = self.handle_er_msg
                                              )
        Trace.init(self.log_name)

        #We need to obtain access to the accounting DB.  Don't define
        # the interface object to the acounting DB here.  Only get the
        # information to contact it here.  The access to the DBs needs to
        # be done locally in the code to avoid threading/forking releated
        # problems.
        self.connect()

        #
        # we need volume clerk to get information about volume when filling
        # drive_utilization table
        # added 07/29 by litvinse@fnal.gov
        #
        self.vcc =  volume_clerk_client.VolumeClerkClient(self.csc,
                                                          rcv_timeout=5,
                                                          rcv_tries=2)

        #Get the configuration from the configuration server.
        ratekeep = self.csc.get(enstore_constants.RATEKEEPER,
                                timeout=15, retry=3)

        ratekeeper_dir  = ratekeep.get('dir', None)
        ratekeeper_host = ratekeep.get('hostip',ratekeep.get('host','MISSING'))
        ratekeeper_port = ratekeep.get('port','MISSING')
        #ratekeeper_nodes = ratekeep.get('nodes','MISSING') #Command line info.
        ratekeeper_addr = (ratekeeper_host, ratekeeper_port)

        self.child_ttl = ratekeep.get('spawned_process_lifetime', CHILD_TTL)
        self.mover_to = ratekeep.get('mover_status_timeout', MVR_RETRY_INTERVAL)
        self.mover_retries = ratekeep.get('mover_status_retries', MVR_RETRIES)
        #if ratekeeper_dir  == 'MISSING' or not ratekeeper_dir:
        #    print "Error: Missing ratekeeper configdict directory.",
        #    print "  (ratekeeper_dir)"
        #    sys.exit(1)
        if ratekeeper_host == 'MISSING' or not ratekeeper_host:
            sys.stderr.write("Error: Missing ratekeeper configdict directory.")
            sys.stderr.write("  (ratekeeper_host)\n")
            sys.exit(1)
        if ratekeeper_port == 'MISSING' or not ratekeeper_port:
            sys.stderr.write("Error: Missing ratekeeper configdict directory.")
            sys.stderr.write("  (ratekeeper_port)\n")
            sys.exit(1)
        #if ratekeeper_nodes == 'MISSING':
        #    ratekeeper_nodes = ''

        dispatching_worker.DispatchingWorker.__init__(self, ratekeeper_addr)

        self.filename_base = socket.gethostname()  #filename_base
        if ratekeeper_dir and os.path.exists(ratekeeper_dir):
            self.output_dir = ratekeeper_dir
        else:
            self.output_dir = None
        self.outfile = None
        self.ymd = None #Year, month, date
        self.last_ymd = None
        self.subscribe_time = 0
        self.mover_msg = {} #key is mover, value is last (num, denom)

        #The generic server __init__ function creates self.erc, but it
        # needs some additional paramaters.
        self.event_relay_subscribe([event_relay_messages.TRANSFER,
                                    event_relay_messages.NEWCONFIGFILE])

        #The event relay client start() function sets up the erc socket to be
        # monitored by the dispatching worker layer.  We do not want this
        # in the ratekeeper.  It monitors this socket on its own, so it
        # must be removed from the list.
        self.remove_select_fd(self.erc.sock)


        self.add_interval_func(self.DRVBusy_interval_func, DRVBUSY_INTERVAL,
                               one_shot=0, align_interval = True)
        self.add_interval_func(self.slots_interval_func, SLOTS_INTERVAL,
                               one_shot=0, align_interval = True)

        self.set_error_handler(self.ratekeeper_error_handler)


    def reinit(self):
        Trace.log(e_errors.INFO, "(Re)initializing server")

        rate_lock.acquire()

        # stop the communications with the event relay task
        self.event_relay_unsubscribe()

        #Close the connections with the database.
        self.close()

        ###We shouldn't need to stop the rk_main thread here.  It will
        ### pick up any relavent configuration changes every 15 seconds.

        self.__init__(self.csc)

        rate_lock.release()

    # close the database connection
    def close(self):
        acc_db_lock.acquire()
        self.acc_db.close()
        acc_db_lock.release()
        return

    #This function is called when dispatching_worker.process_request()
    # throws a traceback (if it is set as the error handler in __init__).
    # This function was copied from file_clerk.py.
    #
    #This function is not reqally used however, since the ratekeeper
    # does not access the database based on user commands.  Should it need
    # to in the future, then this function is ready to go.
    def ratekeeper_error_handler(self, exc, msg, tb):
        __pychecker__ = "unusednames=tb"
        # is it PostgreSQL connection error?
        #
        # This is indeed a OR condition implemented in if-elif-elif-...
        # so that each one can be specified individually
        if exc == pg.ProgrammingError and str(msg)[:13] == 'server closed':
            self.reconnect()
        elif exc == ValueError and str(msg)[:13] == 'server closed':
            self.reconnect()
        elif exc == TypeError and str(msg)[:10] == 'Connection':
            self.reconnect()
        elif exc == ValueError and str(msg)[:13] == 'no connection':
            self.reconnect()
        self.reply_to_caller({'status':(str(exc),str(msg), 'error'),
            'exc_type':str(exc), 'exc_value':str(msg)} )

    # reconnect() -- re-establish connection to database
    def reconnect(self):
        print time.ctime(), "Reconnecting to database."
        self.acc_db.close()
        self.connect()
        print time.ctime(), "Done reestablishing connection to database."

    # establish connection to the database
    def connect(self):
        self.acc_db = None
        while not self.acc_db:
            self.acc_conf = self.csc.get(enstore_constants.ACCOUNTING_SERVER)
            if not e_errors.is_ok(self.acc_conf):
                message = "Unable to get accounting database information: %s" \
                          (self.acc_conf['status'],)
                Trace.log(e_errors.ERROR, message)
            try:
                self.acc_db   = pg.DB(
                    host   = self.acc_conf.get('dbhost', "localhost"),
                    port   = self.acc_conf.get('dbport', 5432),
                    dbname = self.acc_conf.get('dbname', "accounting"),
                    user   = self.acc_conf.get('dbuser', "enstore"),
                    )
            except (pg.ProgrammingError, pg.InternalError):
                exc_type, exc_value = sys.exc_info()[:2]
                message = str(exc_type)+' '+str(exc_value)+' IS POSTMASTER RUNNING?'
                Trace.log(e_errors.ERROR, message.replace("\n", "  "))
                time.sleep(30)
                continue
            except:
                exc_type, exc_value = sys.exc_info()[:2]
                message = str(exc_type)+' '+str(exc_value)
                Trace.log(e_errors.ERROR, message)
                message = "CAN NOT ESTABLISH DATABASE CONNECTION ... QUIT!"
                Trace.log(e_errors.ERROR, message)
                sys.exit(1)

    # These need confirmation
    def quit(self, ticket):
        #Collect children.
        while self.n_children > 0:
            self.collect_children()
            time.sleep(1)
        # stop the communications with the event relay task
        self.event_relay_unsubscribe()
        #Close the connections with the database.
        acc_db_lock.acquire()
        self.acc_db.close()
        acc_db_lock.release()
        #
        dispatching_worker.DispatchingWorker.quit(self, ticket)

    def subscribe(self):
        self.erc.subscribe()

    # Do update the DB every 15 minutes.
    def DRVBusy_interval_func(self):
        #Get the list of media changers from the config server.
        mcs = self.csc.get_media_changers2(timeout = 3, retry = 10)
        for mc in mcs:
            if 'null' in mc['name']:
                continue # dirty way to skip null media changer
            mcc = media_changer_client.MediaChangerClient(
                self.csc, name = mc['name'],
                rcv_timeout = 3, rcv_tries = 3
                )

            if self.fork(self.child_ttl):
                #Parent
                continue

            #child
            self.update_DRVBusy(mcc)
            os._exit(0)

    # Do update the DB every 6 hours.
    def slots_interval_func(self):
        #Get the list of media changers from the config server.
        mcs = self.csc.get_media_changers2(timeout = 3, retry = 10)
        for mc in mcs:
            mcc = media_changer_client.MediaChangerClient(
                self.csc, name = mc['name'],
                rcv_timeout = 3, rcv_tries = 3
                )

            if self.fork(self.child_ttl):
                #Parent
                continue

            #child
            self.update_slots(mcc)
            os._exit(0)

    def update_DRVBusy(self, mcc):
        busy_count = {}
        total_count = {}
        drives_list = []

        now = time.time()

        #First get the name of the tape library.
        tape_library = self.csc.get(mcc.server_name, 5, 5).get('tape_library',
                                                               None)
        if tape_library == None:
            return

        #Get the drives from the media changer.
        drives_dict = mcc.list_drives(10, 6)
        if e_errors.is_ok(drives_dict):
            drives_list = drives_dict['drive_list']
            for d in drives_list:
                if 'address' in d:
                    d['name'] = d['address'] # this is for data for IBM tape library
        #Gather the list of movers listed in the configuration.
        valid_drives = []
        config_dict = self.csc.dump_and_save()
        for conf_key in config_dict.keys():
            if conf_key[-6:] == ".mover":
                #Disk movers don't have 'media_changer'.
                conf_mc = config_dict[conf_key].get('media_changer', None)
                if conf_mc == mcc.server_name:
                    if 'mc_device' in config_dict[conf_key]:
                        # SL tape library
                        valid_drives.append(config_dict[conf_key]['mc_device'])
                    else:
                        # IBM and Spectra Logic tape libraries:
                        # query mover to get info.
                        movc = mover_client.MoverClient(self.csc, conf_key)
                        m_status = movc.status(self.mover_to, self.mover_retries)
                        if e_errors.is_ok(m_status):
                            valid_drives.append(m_status['media_changer_device'])

        for drive in drives_list:
            if not drive['name'] in valid_drives:
                #If the drive isn't attached to a configured mover
                # skip its count.  It probably belongs to another instance
                # of Enstore.
                continue
            try:
                total_count[drive['type']] = \
                                           total_count[drive['type']] + 1
            except:
                total_count[drive['type']] = 1
                #If the total did not have this type yet, then just the
                #busy counts cant have it yet.

            if drive['volume']:
                v_info=self.vcc.inquire_vol(drive['volume'])
                sg=None
                try:
                    sg=volume_family.extract_storage_group(v_info['volume_family'])
                except:
                    exc, msg, tb = sys.exc_info()
                    try:
                        sys.stderr.write("Can not extract storage group for volume %s : (%s, %s)\n" %
                                         (drive['volume'],exc, msg))
                        sys.stderr.flush()
                    except IOError:
                        pass
                    continue
                try:
                    busy_count[(drive['type'], sg)]  =   busy_count[(drive['type'], sg)] + 1
                except:
                    busy_count[(drive['type'], sg)]  =   1

        try:
            acc_db = pg.DB(host  = self.acc_conf.get('dbhost', "localhost"),
                           port  = self.acc_conf.get('dbport', 5432),
                           dbname= self.acc_conf.get('dbname', "accounting"),
                           user  = self.acc_conf.get('dbuser', "enstore"))

            ## Put the information into the accounting DB.
            for drive_type in total_count.keys():
                for k in busy_count.keys():
                    if k[0] == drive_type:
                        q="insert into drive_utilization \
                        (time, tape_library, type, storage_group, total, busy) values \
                        ('%s', '%s', '%s',  '%s', %d,  %d)" % \
                        (time.strftime("%m-%d-%Y %H:%M:%S %Z",
                                       time.localtime(now)),
                         tape_library,
                         drive_type,
                         k[1],
                         total_count[drive_type],
                         busy_count[k],
                         )
                        #print "Executing ",q
                        acc_db.query(q)
            acc_db.close()
        except (pg.ProgrammingError, pg.InternalError):
            exc, msg, tb = sys.exc_info()
            try:
                sys.stderr.write("%s: Can not update DB: (%s, %s)\n" %
                                 (time.ctime(), exc, msg))
                sys.stderr.flush()
            except IOError:
                pass
        return (busy_count, total_count)

    def update_slots(self, mcc):
        slots_list = []

        now = time.time()

        #First get the name of the tape library.
        tape_library = self.csc.get(mcc.server_name, 5, 5).get('tape_library',
                                                               None)
        if tape_library == None:
            return

        slots_dict = mcc.list_slots(10, 18)
        if e_errors.is_ok(slots_dict):
            slots_list = slots_dict['slot_list']

        try:

            ## Put the information into the accounting DB.
            acc_db = pg.DB(host  = self.acc_conf.get('dbhost', "localhost"),
                           port  = self.acc_conf.get('dbport', 5432),
                           dbname= self.acc_conf.get('dbname', "accounting"),
                           user  = self.acc_conf.get('dbuser', "enstore"))

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
        except (pg.ProgrammingError, pg.InternalError,KeyError, TypeError):
            exc, msg, tb = sys.exc_info()
            try:
                sys.stderr.write("%s: Can not update DB: (%s, %s)\n" %
                                 (time.ctime(), exc, msg))
                sys.stderr.flush()
            except IOError:
                pass
        return slots_list

    def check_outfile(self, now=None):
        if now is None:
            now = time.time()
        tup = time.localtime(now)
        self.ymd = tup[:3]
        if self.ymd != self.last_ymd:
            self.last_ymd = self.ymd
            if self.outfile:
                try:
                    self.outfile.close()
                except:
                    try:
                        sys.stderr.write("Can't open file\n")
                        sys.stderr.flush()
                    except IOError:
                        pass

            year, month, day = self.ymd

            if self.output_dir:
                outfile_name = os.path.join(self.output_dir, \
                                        "%s.RATES.%04d%02d%02d" %
                                        (self.filename_base, year, month, day))
                self.outfile=open(outfile_name, 'a')

    def count_bytes(self, words, bytes_read_dict, bytes_written_dict, group):
        mover = words[1]
        mover = string.upper(mover)


        #Get the number of bytes moved (words[2]) and total bytes ([3]).
        num = long(words[2])  #NB -bytes = read;  +bytes=write
        writing = num>0
        num = abs(num)
        denom = long(words[3])

        #Get the last pair of numbers for each mover.
        rate_lock.acquire()
        prev = self.mover_msg.get(mover)
        self.mover_msg[mover] = (num, denom)
        rate_lock.release()

        #When a new file is started, the first transfer occurs, a mover
        # quits, et al, then initialize these parameters.
        if not prev:
            num_0 = denom_0 = 0
        else:
            num_0, denom_0 = prev
        if num_0 >= denom or denom_0 != denom:
            num_0 = denom_0 = 0

        #Caluclate the number of bytes transfered at this time.
        bytes_transfered = num - num_0
        if writing:
            bytes_written_dict[group] = \
                bytes_written_dict.get(group,0L) + bytes_transfered
        else:
            bytes_read_dict[group] = \
                bytes_read_dict.get(group,0L) + bytes_transfered

        #If the file is known to be transfered, reset these to zero.
        if num == denom:
            num_0 = denom_0 = 0

    # return when it is at the beginning of the next minute.
    # Make sure that rate_lock() is locked before calling this function.
    def start_next_minute(self):
        now = time.time()
        self.start_time = next_minute(now)
        wait = self.start_time - now
        print time.ctime(), "waiting %.2f seconds" % (wait,)
        time.sleep(wait)
        print time.ctime(), "starting"

    #main() runs in its own thread.
    def main(self):

        #Wait for the next minute to begin.
        rate_lock.acquire()
        self.start_next_minute()
        rate_lock.release()

        N = 1L
        bytes_read_dict = {} # = 0L
        bytes_written_dict = {} #0L

        while 1:
            now = time.time()

            #Handle resubscription to the event relay.
            rate_lock.acquire()
            self.check_outfile(now)
            if now - self.subscribe_time > self.resubscribe_interval:
                self.subscribe()
                self.subscribe_time = now
            rate_lock.release()

            end_time = self.start_time + N * self.interval
            remaining = end_time - now
            if remaining <= 0:

                rate_lock.acquire()

                ############################################################
                # [DEPRICATED] Write the rate data to the rate log file.
                try:
                    if self.outfile:
                        self.outfile.write( "%s %d %d %d %d\n" %
                                        (time.strftime("%m-%d-%Y %H:%M:%S",
                                                       time.localtime(now)),
                                         bytes_read_dict.get("REAL", 0),
                                         bytes_written_dict.get("REAL", 0),
                                         bytes_read_dict.get("NULL", 0),
                                         bytes_written_dict.get("NULL", 0),))
                        self.outfile.flush()
                except:
                    try:
                        sys.stderr.write("Can not write to output file.\n")
                        sys.stderr.flush()
                    except IOError:
                        pass
                ###########################################################
                # Insert the rate data into the DB.
                acc_db_lock.acquire()
                try:
                    q="insert into rate (time, read, write, read_null, write_null) values \
                       ('%s', %d,  %d,  %d,  %d)"%(time.strftime("%m-%d-%Y %H:%M:%S %Z",
                                                                 time.localtime(now)),
                                                   bytes_read_dict.get("REAL", 0),
                                                   bytes_written_dict.get("REAL", 0),
                                                   bytes_read_dict.get("NULL", 0),
                                                   bytes_written_dict.get("NULL", 0),)
                    self.acc_db.query(q)
                except (pg.ProgrammingError, pg.InternalError):
                    exc, msg, tb = sys.exc_info()
                    try:
                        sys.stderr.write("%s: Can not update DB: (%s, %s)\n" %
                                         (time.ctime(), exc, msg))
                        sys.stderr.flush()
                    except IOError:
                        pass
                    #Attempt to reconnect in 5 seconds.
                    time.sleep(5)
                    self.reconnect()
                    #Wait for the next minute.
                    self.start_next_minute()
                    N = 1L #Reset this back now that self.start_time is reset.
                    #Avoid resource leaks, release the locks.
                    acc_db_lock.release()
                    rate_lock.release()
                    continue

                acc_db_lock.release()
                rate_lock.release()

                for key in bytes_read_dict.keys():
                    bytes_read_dict[key] = 0L
                    bytes_written_dict[key] = 0L

                N = N + 1
                end_time = self.start_time + N * self.interval
                remaining = end_time - now

            if remaining <= 0: #Possible from ntp clock update???
                continue

            rate_lock.acquire()
            r, w, x = select.select([self.erc.sock], [], [], remaining)
            rate_lock.release()

            if not r:
                continue

            r=r[0]

            try:
                cmd = r.recv(1024)
            except:
                cmd = None

            #Take the command and seperate the string spliting on whitespace.
            if not cmd:
                continue
            cmd = string.strip(cmd)
            words = string.split(cmd)
            if not words:
                continue

            if words[0] == event_relay_messages.NEWCONFIGFILE:
                #Hack to get what we want from GenericServer.
                self._reinit2()
                continue
            elif words[0] == event_relay_messages.TRANSFER:
                #If the split strings don't contain the fields we are
                # looking for then ignore them.
                if len(words) < 5: #Don't crash if an old mover is sending.
                    continue
                if words[4] != 'network':
                    #Only transfer messages with network information is used.
                    # Ingore all others and continue onto the next message.
                    continue
            else:
                #Impossible, we don't ask for other types of messages.
                continue

            if string.find(string.upper(words[1]), 'NULL') >= 0:
                self.count_bytes(words, bytes_read_dict,
                                 bytes_written_dict,"NULL")
            else:
                self.count_bytes(words, bytes_read_dict,
                                 bytes_written_dict,"REAL")


class RatekeeperInterface(generic_server.GenericServerInterface):

    def __init__(self):
        self.host = None
        generic_server.GenericServerInterface.__init__(self)

    def valid_dictionaries(self):
        return (self.help_options,)


if __name__ == "__main__":
    intf = RatekeeperInterface()

    rk = Ratekeeper((intf.config_host, intf.config_port))

    reply = rk.handle_generic_commands(intf)

    rk_main_thread = threading.Thread(target=rk.main)
    rk_main_thread.start()

    while 1:
        try:
            Trace.log(e_errors.INFO, "Ratekeeper (re)starting")
            rk.serve_forever()
        except SystemExit, exit_code:
            rk.acc_db.close()
            sys.exit(exit_code)
        except:
            Trace.handle_error()
            rk.serve_forever_error("ratekeeper")
            continue

    Trace.log(e_errors.ERROR,"Ratekeeper finished (impossible)")

