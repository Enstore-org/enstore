import time
import callback
import dict_to_a
from configuration_client import configuration_client
from udp_client import UDPClient
from db import do_backup
class VolumeClerkClient :

    def __init__(self, configuration_client) :
        self.csc = configuration_client
        self.u = UDPClient()


    # send the request to the volume clerk server and then send answer to user
    def send (self, ticket) :
        vticket = self.csc.get("volume_clerk")
        return  self.u.send(ticket, (vticket['host'], vticket['port']))


    # add a volume to the stockpile
    def addvol(self,
               library,               # name of library media is in
               file_family,           # volume family the media is in
               media_type,            # media
               external_label,        # label as known to the system
               capacity_bytes,        #
               remaining_bytes,       #
               eod_cookie  = "none",  # code for seeking to eod
               user_inhibit  = "none",# "none" | "readonly" | "all"
               error_inhibit = "none",# "none" | "readonly" | "all" | "writing"
                                      # lesser access is specified as
                                      #       we find media errors,
                                      # writing means that a mover is
                                      #       appending or that a mover
                                      #       crashed while writing
               last_access = -1,      # last accessed time
               first_access = -1,     # first accessed time
               declared = -1,         # time volume was declared to system
               sum_wr_err = 0,        # total number of write errors
               sum_rd_err = 0,        # total number of read errors
               sum_wr_access = 0,     # total number of write mounts
               sum_rd_access = 0,     # total number of read mounts
               wrapper = "cpio",      # kind of wrapper for volume
               blocksize = -1         # blocksize (-1 =  media type specifies)
               ) :
        ticket = { 'work'            : 'addvol',
                   'library'         : library,
                   'file_family'     : file_family,
                   'media_type'      : media_type,
                   'external_label'  : external_label,
                   'capacity_bytes'  : capacity_bytes,
                   'remaining_bytes' : remaining_bytes,
                   'eod_cookie'      : eod_cookie,
                   'user_inhibit'    : user_inhibit,
                   'error_inhibit'   : error_inhibit,
                   'last_access'     : last_access,
                   'first_access'    : first_access,
                   'declared'        : declared,
                   'sum_wr_err'      : sum_wr_err,
                   'sum_rd_err'      : sum_rd_err,
                   'sum_wr_access'   : sum_wr_access,
                   'sum_rd_access'   : sum_rd_access,
                   'wrapper'         : wrapper,
                   'blocksize'       : blocksize }
        return self.send(ticket)


    # delete a volume from the stockpile
    def delvol(self, external_label) :
        ticket= { 'work'           : 'delvol',
                  'external_label' : external_label }
        return  self.send(ticket)


    # get a list of all volumnes


    def get_vols(self):
	import string
        # get a port to talk on and listen for connections
        host, port, listen_socket = callback.get_callback()
        listen_socket.listen(4)
        ticket = {"work"               : "get_vols",
                  "user_callback_port" : port,
                  "user_callback_host" : host,
                  "unique_id"          : time.time() }
        # send the work ticket to the library manager
        ticket = self.send(ticket)
        if ticket['status'] != "ok" :
            raise errno.errorcode[errno.EPROTO],"vcc.get_vols: sending ticket"\
                  +repr(ticket)

        # We have placed our request in the system and now we have to wait.
        # All we  need to do is wait for the system to call us back,
        # and make sure that is it calling _us_ back, and not some sort of old
        # call-back to this very same port. It is dicey to time out, as it
        # is probably legitimate to wait for hours....
        while 1 :
            control_socket, address = listen_socket.accept()
            new_ticket = callback.read_tcp_socket(control_socket, "volume"+\
                                  "clerk client get_vols,  vc call back")
            import pprint
            if ticket["unique_id"] == new_ticket["unique_id"] :
                listen_socket.close()
                break
            else:
                print ("vcc.get_vols: imposter called us back, trying again")
                control_socket.close()
        ticket = new_ticket
        if ticket["status"] != "ok" :
            raise errno.errorcode[errno.EPROTO],"vcc.get_vols: "\
                  +"1st (pre-work-read) volume clerk callback on socket "\
                  +repr(address)+", failed to setup transfer: "\
                  +"ticket[\"status\"]="+ticket["status"]

        # If the system has called us back with our own  unique id, call back
        # the library manager on the library manager's port and read the
        # work queues on that port.
        data_path_socket = callback.volume_clerk_callback_socket(ticket)
	ticket= callback.read_tcp_socket(data_path_socket, "volume clerk"\
		  +"client get_vols, vc final dialog")
#	workmsg=""
        while 1:
	  msg=callback.read_tcp_buf(data_path_socket,"volume clerk "+"client get_vols, reading worklist")
	  if len(msg)==0 :
#		pprint.pprint(workmsg)
		break
#	  workmsg=workmsg+msg
#	  pprint.pprint(workmsg[:string.rfind(workmsg,',',0)])
#	  workmsg=msg[string.rfind(msg,',',0)+1:]
	  pprint.pprint(msg)
#	ticket['vols']=workmsg
	worklist = ticket
	data_path_socket.close()


        # Work has been read - wait for final dialog with volume clerk
        done_ticket = callback.read_tcp_socket(control_socket, "volume clerk"\
		  +"client get_vols, vc final dialog")
        control_socket.close()
        if done_ticket["status"] != "ok" :
            raise errno.errorcode[errno.EPROTO],"vcc.get_vols "\
                  +"2nd (post-work-read) volume clerk callback on socket "\
                  +repr(address)+", failed to transfer: "\
                  +"ticket[\"status\"]="+ticket["status"]
        return worklist

    # what is the current status of a specified volume?
    def inquire_vol(self, external_label) :
        ticket= { 'work'           : 'inquire_vol',
                  'external_label' : external_label }
        return  self.send(ticket)

    # we are using the volume
    def set_writing(self, external_label) :
        ticket= { 'work'           : 'set_writing',
                  'external_label' : external_label }
        return self.send(ticket)

    # we are using the volume
    def set_system_readonly(self, external_label) :
        ticket= { 'work'           : 'set_system_readonly',
                  'external_label' : external_label }
        return self.send(ticket)

    # clear any inhibits on the volume
    def clr_system_inhibit(self,external_label):
        ticket= { 'work'           : 'clr_system_inhibit',
                  'external_label' : external_label }
        return self.send(ticket)

    # we are using the volume
    def set_hung(self, external_label) :
        ticket= { 'work'           : 'set_hung',
                  'external_label' : external_label }
        return self.send(ticket)

    # this many bytes left - update database
    def set_remaining_bytes(self, external_label,remaining_bytes,eod_cookie,
                            wr_err,rd_err,wr_access,rd_access) :
        ticket= { 'work'            : 'set_remaining_bytes',
                  'external_label'  : external_label,
                  'remaining_bytes' : remaining_bytes,
                  'eod_cookie'      : eod_cookie,
                  'wr_err'          : wr_err,
                  'rd_err'          : rd_err,
                  'wr_access'       : wr_access,
                  'rd_access'       : rd_access }
        return self.send(ticket)

    # update the counts in the database
    def update_counts(self, external_label, wr_err,rd_err,wr_access,rd_access):
        ticket= { 'work'            : 'update_counts',
                  'external_label'  : external_label,
                  'wr_err'          : wr_err,
                  'rd_err'          : rd_err,
                  'wr_access'       : wr_access,
                  'rd_access'       : rd_access }
        return self.send(ticket)

    # which volume can we use for this library, bytes and file family and ...
    def next_write_volume (self, library, min_remaining_bytes,
                           file_family, vol_veto_list,first_found) :
 

        return self.send(ticket)


    # check on alive status
    def alive(self):
        return self.send({'work':'alive'})
    def start_backup(self):
    	return self.send({'work':'start_backup'})
    def stop_backup(self):
    	return self.send({'work':'stop_backup'})	



if __name__ == "__main__" :
    import sys
    import getopt
    import string
    import pprint
    # Import SOCKS module if it exists, else standard socket module socket
    # This is a python module that works just like the socket module, but uses
    # the SOCKS protocol to make connections through a firewall machine.
    # See http://www.w3.org/People/Connolly/support/socksForPython.html or
    # goto www.python.org and search for "import SOCKS"
    try:
        import SOCKS; socket = SOCKS
    except ImportError:
        import socket

    # defaults
    #config_host = "localhost"
    (config_host,ca,ci) = socket.gethostbyaddr(socket.gethostname())
    config_port = "7500"
    config_list = 0
    vol = ""
    vols = 0
    nextvol = 0
    list = 0
    addvol = 0
    delvol = 0
    clrvol = 0
    backup=0
    alive = 0

    # see what the user has specified. bomb out if wrong options specified
    options = ["config_host=","config_port=","config_list",
               "vols","nextvol","vol=","addvol","delvol","list","verbose",
               "clrvol","alive","backup","help"]
    optlist,args=getopt.getopt(sys.argv[1:],'',options)
 
    for (opt,value) in optlist :
        if opt == "--config_host" :
            config_host = value
        elif opt == "--config_port" :
            config_port = value
        elif opt == "--config_list" :
            config_list = 1
        elif opt == "--vols" :
            vols = 1
        elif opt == "--nextvol" :
            nextvol = 1	
        elif opt == "--vol" :
            vol = value
        elif opt == "--addvol" :
            addvol = 1
        elif opt == "--delvol" :
            delvol = 1
        elif opt == "--clrvol" :
            clrvol = 1
        elif opt == "--alive" :
            alive = 1
        elif opt == "--list" or opt == "--verbose":
            list = 1
	elif opt == "--backup":
	    backup = 1
        elif opt == "--help" :
            print "python ",sys.argv[0], options
            print "   do not forget the '--' in front of each option"
            print "   addvol arguments: library file_family media_type"\
                  +", volume_name, volume_byte_capacity remaining_capacity"
            print "   delvol arguments: volume_name"
            sys.exit(0)

    # bomb out if can't translate host
    ip = socket.gethostbyname(config_host)

    # bomb out if port isn't numeric
    config_port = string.atoi(config_port)

    if config_list :
        print "Connecting to configuration server at ",config_host,config_port
    csc = configuration_client(config_host,config_port)

    vcc = VolumeClerkClient(csc)

    if alive:
        ticket = vcc.alive()
    elif backup:
	ticket = vcc.start_backup()
	do_backup("volume")
	ticket = vcc.stop_backup()
    elif vols :
        ticket = vcc.get_vols()
    elif nextvol:
	ticket = vcc.next_write_volume(args[0], #library
				       string.atol(args[1]), #min_remaining_byte
				       args[2], #file_family
				            [], #vol_veto_list
                                             1) #first_found

    elif vol :
        ticket = vcc.inquire_vol(vol)
    elif addvol:
        # bomb out if we don't have correct number of add vol arguments
        if len(args) < 6 :
            print "   addvol arguments: library file_family media_type"\
                  +", volume_name, volume_byte_capacity remaining_capacity"
            sys.exit(1)
        ticket = vcc.addvol(args[0],              # library
                            args[1],              # file family
                            args[2],              # media type
                            args[3],              # name of this volume
                            string.atol(args[4]), # cap'y of this vol (bytes)
                            string.atol(args[5])) # rem cap'y of this volume
    elif delvol:
        # bomb out if we don't have correct number of del vol arguments
        if len(args) < 1 :
            print "   delvol arguments: volume_name"
            sys.exit(1)
        ticket = vcc.delvol(args[0])              # name of this volume

    elif clrvol:
        # bomb out if we don't have correct number of clr_inhibit arguments
        if len(args) < 1 :
            print "   clr_inhibit arguments: volume_name"
            sys.exit(1)
        ticket = vcc.clr_system_inhibit(args[0])  # name of this volume

    if ticket['status'] != 'ok' :
        print "Bad status:",ticket['status']
        pprint.pprint(ticket)
        sys.exit(1)
    elif list:
        pprint.pprint(ticket)
        sys.exit(0)










