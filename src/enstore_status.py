#
# system import
import sys
import time
import pprint
import copy
import errno
import regsub
import string

# enstore imports
import traceback
import Trace
import e_errors

def default_timeout():
    return 60

def default_alive_rcv_timeout():
    return 5

def default_alive_retries():
    return 2

def default_file_dir():
    return "./"

class EnstoreStatus:

    def __init__(self, dir=default_file_dir(), list=0):
        Trace.trace(10,'{__init__ essfile')
	if dir == "":
	    dir = "/tmp"
        self.file_name = dir + "/" + "enstore_system_status.txt"
        if list :
            print "opening " + self.file_name
        # try to open status file for append
        try:
            self.file = open(self.file_name, 'a')
            if list :
                print "opened for append"
        except:
            self.file = open(self.file_name, 'w')
            if list :
                print "opened for write"
        Trace.trace(10,'}__init__')

    # output the passed alive status
    def output_alive(self, host, tag, status):
        Trace.trace(12,"{output_alive "+repr(tag)+" "+repr(host))
	str = tag+self.unquote(status['work'])+" at "+self.format_ip_address(host, status['address'])+" is "+self.format_status(status['status'])+"\n"
	self.file.write(str)
        Trace.trace(12,"}output_alive")

    # format the status, just use the first element
    def format_status(self, status):
        Trace.trace(12,"{format_status "+repr(status))
	return self.unquote(status[0])
        Trace.trace(12,"}format_status ")

    # format the ip address - replace the ip address with the actual host name
    def format_ip_address(self, host, address):
        Trace.trace(12,"{format_ip_address "+repr(host)+" "+repr(address))
	return "("+self.unquote(host)+", "+repr(address[1])+")"
        Trace.trace(12,"}format_ip_address ")

    # output the timeout error
    def output_etimedout(self, address, tag):
        Trace.trace(12,"{output_etimedout "+repr(tag)+" "+repr(address))
	stat = tag + "timed out at "+self.unquote(repr(address))+"\n"
	self.file.write(stat)
        Trace.trace(12,"}output_etimedout")

    # get the current time and output it
    def output_time(self):
        Trace.trace(12,"{output_time "+repr(self.file_name))
	tm = time.localtime(time.time())
	atm = "\nENSTORE SYSTEM STATUS at %04d-%02d-%02d %02d:%02d:%02d\n" % (tm[0], tm[1], tm[2], tm[3], tm[4], tm[5])
 	self.file.write(atm)
        Trace.trace(12,"}output_time ")

    # output the library manager queues
    def output_lmqueues(self, ticket):
        Trace.trace(12,"{output_lmqueues "+repr(ticket))
	self.file.write(self.format_lm_queues(ticket))
        Trace.trace(12,"}output_lmqueues ")

    # output the library manager mover list
    def output_lmmoverlist(self, ticket):
        Trace.trace(12,"{output_lmmoverlist "+repr(ticket))
	self.file.write(self.format_lm_moverlist(ticket))
        Trace.trace(12,"}output_lmmoverlist ")

    # output the name of the server
    def output_name(self, name):
        Trace.trace(12,"{output_name "+repr(name))
	self.file.write(self.unquote(name))
        Trace.trace(12,"}output_name ")

    # remove all single quotes
    def unquote(self, string):
        Trace.trace(12,"{unquote "+repr(string))
	return regsub.gsub("\'", "", string)
        Trace.trace(12,"}unquote ")

    # translate time.time output to a person readable format.
    # strip off the day and reorganize things a little
    def format_time(self, theTime):
	ftime = time.strftime("%c", time.localtime(theTime))
	(dow, mon, day, tod, year) = string.split(ftime)
	ntime = "%s-%s-%s %s" % (year, mon, day, tod)
	return ntime

    # parse the library manager queues returned from "getwork"
    def parse_lm_queues(self, work, spacing, prefix):
	Trace.trace(13,"{parse_lm_queues")
	for mover in work:
	    callback_addr = mover['callback_addr']
	    encp = mover['encp']
	    # not found in pending work
	    try:
	        fc = mover['fc']
	    except:
	        pass
	    times = mover['times']
	    vc = mover['vc']
	    wrapper = mover['wrapper']
	    machine = wrapper['machine']

	    string = prefix
	    # not found in pending work
	    try:
	        string = string+mover['mover']+", "
	    except:
	        pass
	    string = string+"from NODE: "+self.unquote(machine[1])+" ("+\
	             self.unquote(machine[0])+"),  PORT: "+\
	             repr(callback_addr[1])
	    if mover['work'] == 'write_to_hsm':
	        string = string+spacing+"WRITE to: "
	    else:
		string = string+spacing+"READ to: "
	    string = string+wrapper['fullname']+",  BYTES: "+\
	             repr(wrapper['size_bytes'])
	    # not found in pending work
	    try:
	        string = string+spacing+"DEVICE LABEL: "+fc['external_label']+\
	             ",  "
	    except:
	        string = string+spacing
	    string = string+"FILE FAMILY: "+vc['file_family']
	    # not found in reads
	    try:
	        string = string+",  FILE FAMILY WIDTH: "+\
	                 repr(vc['file_family_width'])
	    except:
	        pass
	    string = string+spacing+"PRIORITIES:  CURRENT "+\
	             repr(encp['curpri'])+",  BASE "+repr(encp['basepri'])+\
	             ",  DELTA "+repr(encp['delpri'])+"  and  AGETIME: "+\
	             repr(encp['agetime'])
	    string = string+spacing+"JOB SUBMITTED: "+\
	             self.format_time(times['t0'])
	    # not found in pending work
	    try:
	        string = string+",  DEQUEUED: "+\
	                 self.format_time(times['lm_dequeued'])
	    except:
	        pass
	    # not found in reads
	    try:
	        string = string+spacing+"FILE MODIFIED: "+\
	                 self.format_time(wrapper['mtime'])
	    except:
	        pass
	string = string+"\n"
	Trace.trace(13,"}parse_lm_queues")
	return string

    # format the library manager work queues for output
    def format_lm_queues(self, ticket):
        Trace.trace(12,"{format_lm_queues "+repr(ticket))
	string = "    Work for: "
	spacing = "\n          "
	work = ticket['at movers']
	if len(work) != 0:
	    string = self.parse_lm_queues(work, spacing, string)
	else:
	    string = "    No work at movers\n"
	pending_work = ticket['pending_work']
	if len(pending_work) != 0:
	    string = string+"\n    Pending work: "
	    string = self.parse_lm_queues(pending_work, spacing, string)
	else:
	    string = string+"    No pending work\n"

        Trace.trace(12,"}format_lm_queues ")
	return string

    # parse the library manager moverlist ticket
    def parse_lm_moverlist(self, work):
        Trace.trace(13,"{parse_lm_moverlist")
	string = "    KNOWN MOVER           PORT    STATE         LAST CHECKED         TRY COUNT\n"
	for mover in work:
	    (address, port) = mover['address']
	    time = self.format_time(mover['last_checked'])
	    string = string+"    %(m)-18.18s    %(p)-4.4d    %(s)-10.10s    %(lc)-20.20s    %(tc)-3d\n" % {'m':mover['mover'], 'p':port, 's':mover['state'], 'lc':time, 'tc':mover['summon_try_cnt']}

	string = string+"\n"
        Trace.trace(13,"}parse_lm_moverlist")
	return string

    # format the library manager mover list for output
    def format_lm_moverlist(self, ticket):
        Trace.trace(12,"{format_lm_moverlist "+repr(ticket))
	string = "    Known Movers: "
	spacing = "\n                 "
	work = ticket['moverlist']
	if len(work) != 0:
	    string = self.parse_lm_moverlist(work)
	else:
	    string = "    No moverlist\n"

        Trace.trace(12,"}format_lm_moverlist ")
	return string

    # flush everything to the file
    def flush(self):
        Trace.trace(10,'{flush')
	self.file.flush()
        Trace.trace(10,'}flush')
