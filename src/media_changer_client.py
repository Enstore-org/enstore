###############################################################################
# src/$RCSfile$   $Revision$
#
#########################################################################
#                                                                       #
# Media Changer client.                                                 #
# Media Changer access methods                                          #
#                                                                       #
#########################################################################

#system imports
import sys
import string

#enstore imports
import udp_client
import interface
import generic_client
import Trace
import volume_clerk_client
import e_errors

MY_NAME = ".MC"

class MediaChangerClient(generic_client.GenericClient):
    def __init__(self, csc, name=""):
        self.media_changer=name
        self.log_name = "C_"+string.upper(string.replace(name,
                                                         ".media_changer",
                                                         MY_NAME))
        generic_client.GenericClient.__init__(self, csc, self.log_name)

        self.u = udp_client.UDPClient()

    # send the request to the Media Changer server and then send answer to user
    #      rcv_timeout is set to 300sec, the STK mnt/dismnt time is ~35 sec.   This
    #      should really be a function of which media changer we are talking to.
    # If tries is set to 0, then we only try once -- which we should never do
    # with udp.
    def send (self, ticket, rcv_timeout=300, tries=10) :
        vticket = self.csc.get(self.media_changer)
        return  self.u.send(ticket, (vticket['hostip'], vticket['port']), rcv_timeout, tries)

    def loadvol(self, vol_ticket, mover, drive, vcc):
        ticket = {'work'           : 'loadvol',
                  'vol_ticket'     : vol_ticket,
                  'drive_id'       : drive
                  }
	rt = self.send(ticket)
	if rt['status'][0] == e_errors.OK:
	    v = vcc.set_at_mover(vol_ticket['external_label'], 'mounted',mover)
	    if v['status'][0] != e_errors.OK:
		format = "cannot change to 'mounted' vol=%s mover=%s state=%s"
		Trace.log( e_errors.INFO, format%(vol_ticket["external_label"],
						  v['at_mover'][1],
						  v['at_mover'][0]) )
	    rt['status'] =  v['status']
        return rt

    def unloadvol(self, vol_ticket, mover, drive, vcc):
        ticket = {'work'           : 'unloadvol',
                  'vol_ticket' : vol_ticket,
                  'drive_id'       : drive
                  }
	rt = self.send(ticket)
	if rt['status'][0] == e_errors.OK:
	    v = vcc.set_at_mover(vol_ticket['external_label'], 'unmounted', 
				mover)
	    if v['status'][0] != e_errors.OK:
		format = "cannot change to 'unmounted' vol=%s mover=%s state=%s"
		Trace.log(e_errors.INFO, format%(vol_ticket["external_label"],
						 v['at_mover'][1],
						 v['at_mover'][0]) )
		rt['status'] =  v['status']
		return rt
        return rt

    def viewvol(self, ticket):
	rt = self.send(ticket)
        return rt

    def insertvol(self, IOarea, inNewLib):
        ticket = {'work'         : 'insertvol',
	          'IOarea_name'  : IOarea
		 }
	if inNewLib != "" :
	    ticket['newlib'] = inNewLib
	rt = self.send(ticket)
        return rt

    def ejectvol(self, media_type, volumeList):
        ticket = {'work'         : 'ejectvol',
	          'volList'      : volumeList,
	          'media_type'   : media_type
                  }
	rt = self.send(ticket)
        return rt

    def MaxWork(self, maxwork):
        ticket = {'work'           : 'maxwork',
                  'maxwork'        : maxwork
                 }
        return self.send(ticket)

    def GetWork(self):
        ticket = {'work'           : 'getwork'
                 }
        return self.send(ticket)

class MediaChangerClientInterface(generic_client.GenericClientInterface):
    def __init__(self):
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
        self.media_changer = ""
        self.getwork=0
        self.maxwork=-1
        self.volume = 0
	self.view = 0
	self.insertvol = 0
	self.insertNewLib = ""
	self.ejectvol = 0
	self.viewattrib = 0
        self.drive = 0
        generic_client.GenericClientInterface.__init__(self)

    # define the command line options that are valid
    def options(self):
        return self.client_options()+\
               ["maxwork=","view=","getwork","insertvol",
	        "ejectvol","insertlib="]
    #  define our specific help
    def parameters(self):
        if 0: print self.keys() #lint fix
        return "media_changer"

    # parse the options like normal but make sure we have other args
    def parse_options(self):
        interface.Interface.parse_options(self)
	if self.insertvol:
            if len(self.args) < 1:
	        self.missing_parameter("media_changer")
                self.print_help()
                sys.exit(1)
            else:
                self.media_changer = self.args[0]
		if len(self.args) == 2:
		    self.ioarea = self.args[1]
		else:
		    self.ioarea = None
	elif self.ejectvol:
            if len(self.args) < 3:
	        self.missing_parameter("media_changer/media_type/volumeList")
                self.print_help()
                sys.exit(1)
            else:
                self.media_changer = self.args[0]
                self.media_type = self.args[1]
                self.volumeList = self.args[2]
	else:
            if len(self.args) < 1 :
	        self.missing_parameter("media_changer")
                self.print_help()
                sys.exit(1)
            else:
                self.media_changer = self.args[0]
        if (self.alive == 0) and (self.maxwork==-1) and \
           (self.getwork==0) and (self.view == 0) and \
           (self.insertvol==0) and (self.ejectvol == 0) and \
	   (self.viewattrib == 0):
            # bomb out if number of arguments is wrong
            self.print_help()
	    sys.exit(1)

    # print out our extended help
    def print_help(self):
        interface.Interface.print_help(self)
        print "        --maxwork=N        Max simultaneous operations allowed (may be 0)"
        print "        --getwork          List oprations in progress"
        
if __name__ == "__main__" :
    Trace.init("MEDCH CLI")
    Trace.trace(6,"mcc called with args "+repr(sys.argv))
    
    # fill in the interface
    intf = MediaChangerClientInterface()

    # get a media changer client
    mcc = MediaChangerClient((intf.config_host, intf.config_port),
                             intf.media_changer)
    Trace.init(mcc.get_name(mcc.log_name))

    if intf.alive:
        ticket = mcc.alive(intf.media_changer, intf.alive_rcv_timeout,
                           intf.alive_retries)
    elif intf.view:
        # get a volume clerk client
        vcc = volume_clerk_client.VolumeClerkClient(mcc.csc)
        v_ticket = vcc.inquire_vol(intf.view)
	ticket=mcc.viewvol(v_ticket)
	del vcc
    elif intf.insertvol:
	ticket=mcc.insertvol(intf.ioarea, intf.insertNewLib)
    elif intf.ejectvol:
        ticket=mcc.ejectvol(intf.media_type, intf.volumeList)
    elif intf.maxwork  >= 0:
        ticket=mcc.MaxWork(intf.maxwork)
    elif intf.getwork:
        ticket=mcc.GetWork()
        #print repr(ticket['worklist'])
    else:
        intf.print_help()
        sys.exit(0)

    del mcc.csc.u
    del mcc.u		# del now, otherwise get name exception (just for python v1.5???)

    mcc.check_ticket(ticket)
