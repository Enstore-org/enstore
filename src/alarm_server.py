#
# system import
import sys
import socket
import os

# enstore imports
import log_client
import configuration_client
import dispatching_worker
import interface
import enstore_status
import generic_server
import Trace
import alarm
import e_errors

def default_alive_rcv_timeout():
    return 5

def default_alive_retries():
    return 2

DEFAULT_FILE_NAME = "/enstore_alarms.txt"
DEFAULT_PATROL_FILE_NAME = "/enstore_patrol.txt"
DEFAULT_SUSP_VOLS_THRESH = 3

SERVER = "server"
SUS_VOLS = "suspect_volumes"
LM = "library_manager"

SEVERITY = "severity"
ROOT_ERROR = "root_error"

class AlarmServerMethods(dispatching_worker.DispatchingWorker):

    # pull out alarm info from the ticket and raise the alarm
    def post_alarm(self, ticket):
        if ticket.has_key(SEVERITY):
            severity = ticket[SEVERITY]
            # remove this entry from the dictionary, so if won't be included
            # as part of alarm_info
            del ticket[SEVERITY]
        else:
            severity = e_errors.DEFAULT_SEVERITY
        if ticket.has_key(ROOT_ERROR):
            root_error = ticket[ROOT_ERROR]
            # remove this entry from the dictionary, so if won't be included
            # as part of alarm_info
            del ticket[ROOT_ERROR]
        else:
            root_error = e_errors.DEFAULT_ROOT_ERROR
        # remove this entry from the dictionary, so it won't be included
        # as part of alarm_info
        if ticket.has_key("work"):
            del ticket["work"]

        theAlarm = self.alarm(severity, root_error, ticket)

        # send the reply to the client
        ret_ticket = { 'status' : (e_errors.OK, None),
                       'alarm'  : repr(theAlarm.get_id()) }
        self.send_reply(ret_ticket)

    # raise the alarm
    def alarm(self, severity=e_errors.DEFAULT_SEVERITY,\
              root_error=e_errors.DEFAULT_ROOT_ERROR, alarm_info={}):
        # find out where the alarm came from
        try:
            host = socket.gethostbyaddr(self.reply_address[0])[0]
        except:
            host = str(sys.exc_info()[1])
        # get a new alarm
        theAlarm = alarm.Alarm(host, severity, root_error, alarm_info)
        # save it in memory for us now
        self.alarms[theAlarm.get_id()] = theAlarm
        # write it to the persistent file
        self.write_alarm_file(theAlarm)
        # write it out to the patrol file
        self.write_patrol_file()
        return theAlarm

    def resolve_alarm(self, ticket):
        # an alarm is being resolved, we must do the following -
        #      remove it from our alarm dictionary
        #      rewrite the entire enstore_alarm file
        #      rewrite the enstore patrol file
        #      log this fact
        pass
        
    def ens_status(self, ticket):
        # we have been sent some status.  we must examine it for an error
        # condition and possibly save it for future reference.  following
        # are the things that we look for -
        #      server has crashed
        #      server crashes more than 'n' times per 't' time
        #      an lm's known movers fall below a threshold
        #      an lm's suspect vols rises above a threshold
        #      the number of drives falls below a threshold
        if ticket.has_key(SUS_VOLS):
            self.check_sus_vols(ticket)

        # send the reply to the client
        ret_ticket = { 'status'   : (e_errors.OK, None) }
        self.send_reply(ret_ticket)

    def check_sus_vols(self, ticket):
        # only go through the effort if there are suspect volumes
        if not ticket[SUS_VOLS] == []:
            # do not record this alarm more than once
            akey = ticket[SERVER]+SUS_VOLS
            if not self.info_alarms.has_key(akey):
                lm = self.csc.get(ticket[SERVER])
                # find out what the suspect volume threshold is, it is either
                # in the library managers config dict or use the one for the
                # alarm server
                cthresh = lm.get("sus_vol_thresh", self.sus_vol_thresh)
                if len(ticket[SUS_VOLS]) >= cthresh:
                    ainfo = self.make_alarm_info()
                    ainfo.update({SUS_VOLS : ticket[SUS_VOLS],\
                                  LM : ticket[SERVER] })
                    self.alarm(e_errors.WARNING, e_errors.TOOMANYSUSPVOLS,
                               ainfo)
                    self.info_alarms[akey] = 1

    def make_alarm_info(self):
        return {"source" : "ALRMS-INQ",
                "uid" : self.uid,
                "pid" : self.pid }
    
    def write_alarm_file(self, alarm):
        self.alarm_file.open()
        self.alarm_file.write(alarm)
        self.alarm_file.close()

    def write_patrol_file(self):
        if not self.alarms == {}:
            keys = self.alarms.keys()
            keys.sort()
            self.patrol_file.open()
            for key in keys:
                self.patrol_file.write(self.alarms[key])
            else:
                self.patrol_file.close()
        else:
            # there are no alarms raised.  if the patrol file exists, we
            # need to delete it
            self.patrol_file.remove()

    def get_log_path(self):
        log = self.csc.get("logserver")
        if log.has_key("log_file_path"):
            return log["log_file_path"]
        else:
            return "."
        
    # read the persistent alarm file if it exists.  this reads in all the
    # alarms that have not been resolved.
    def get_alarm_file(self):
        # the alarm file lives in the same directory as the log file
        self.alarm_file = enstore_status.EnAlarmFile(self.get_log_path()+ \
                                                     DEFAULT_FILE_NAME)
        self.alarm_file.open('r')
        self.alarms = self.alarm_file.read()
        self.alarm_file.close()

    # get the location of the file we will create for patrol and write any
    # alarms to it
    def get_patrol_file(self):
        # the patrol file lives in the same directory as the log file
        self.patrol_file = enstore_status.EnPatrolFile(self.get_log_path()+ \
                                                      DEFAULT_PATROL_FILE_NAME)
        self.write_patrol_file()

    def handle_alarms(self):
        pass
               
    # return the name of the patrol file
    def get_patrol_filename(self, ticket):
        ticket['patrol_file'] = self.patrol_file.real_file_name
        ticket['status'] = (e_errors.OK, None)
        self.send_reply(ticket)

class AlarmServer(AlarmServerMethods, generic_server.GenericServer):

    def __init__(self, csc=0, verbose=0, host=interface.default_host(), \
                 port=interface.default_port()):
        self.alarms = {}
        self.info_alarms = {}
        self.print_id = "ALRMS"
        self.verbose = verbose
        self.uid = os.getuid()
        self.pid = os.getpid()

	# get the config server
        configuration_client.set_csc(self, csc, host, port, verbose)
        keys = self.csc.get("alarm_server")
        self.hostip = keys['hostip']
        Trace.init(keys["logname"])
        self.sus_vol_thresh = keys.get("susp_vol_thresh",
                                       DEFAULT_SUSP_VOLS_THRESH)
        self.print_id = keys.get("logname", self.print_id)
        dispatching_worker.DispatchingWorker.__init__(self, (keys['hostip'], \
	                                              keys['port']))
        # get a logger
        self.logc = log_client.LoggerClient(self.csc, keys["logname"], \
                                            'logserver', 0)

        # see if an alarm file exists. if it does, open it and read it in.
        # these are the alarms that have not been dealt with.
        self.get_alarm_file()

        # get the patrol file name and location and write any current alarms
        # out to it.
        self.get_patrol_file()

        # look through all the alarms and see if we need to do anything with
        # them
        self.handle_alarms()

class AlarmServerInterface(interface.Interface):

    def __init__(self):
        # fill in the defaults for possible options
        self.verbose = 0
        interface.Interface.__init__(self)

        # now parse the options
        self.parse_options()

    # define the command line options that are valid
    def options(self):
        return self.config_options()+\
	       ["verbose="] +\
	       self.help_options()

if __name__ == "__main__":
    Trace.init("alarm_server")
    Trace.trace( 6, "alarm server called with args "+repr(sys.argv) )

    # get interface
    intf = AlarmServerInterface()

    # get the alarm server
    als = AlarmServer(0, intf.verbose, intf.config_host, intf.config_port)

    while 1:
        try:
            Trace.trace(1,'Alarm Server (re)starting')
            als.logc.send(e_errors.INFO, 1, "Alarm Server (re)starting")
            als.serve_forever()
        except:
	    als.serve_forever_error("alarm_server", als.logc)
            continue
    Trace.trace(1,"Alarm Server finished (impossible)")
