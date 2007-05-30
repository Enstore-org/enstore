#!/usr/bin/env python

###############################################################################
#
# $Id$
#
################################################################################
#########################################################################
#                                                                       #
# Media Changer client.                                                 #
# Media Changer access methods                                          #
#                                                                       #
#########################################################################

#system imports
import sys
import string
import types
import pprint
import time
import select

#enstore imports
#import udp_client
import option
import generic_client
import Trace
import volume_clerk_client
import e_errors
import callback
import configuration_client
import enstore_functions2

MY_NAME = ".MC"
RCV_TIMEOUT = 0
RCV_TRIES = 0

class MediaChangerClient(generic_client.GenericClient):
    #The paramater 'name' is expected to be something like
    # '9940.media_changer'.
    def __init__(self, csc, name="", flags=0, logc=None, alarmc=None,
                 rcv_timeout = RCV_TIMEOUT, rcv_tries = RCV_TRIES):
        self.media_changer = name
        self.log_name = "C_"+string.upper(string.replace(name,
                                                         ".media_changer",
                                                         MY_NAME))
        generic_client.GenericClient.__init__(self, csc, self.log_name,
                                              flags = flags, logc = logc,
                                              alarmc = alarmc,
                                              rcv_timeout = rcv_timeout,
                                              rcv_tries = rcv_tries,
                                              server_name = name)
        #if name:
        #    self.server_address = self.get_server_address(name)

    ##These functions should really take a named parameter list rather than "vol_ticket".  It's
    ## not clear what keys need to be present in vol_ticket.  Looks like
    ## at least external_label and media_type are needed
    def loadvol(self, vol_ticket, mover, drive):
        ticket = {'work'           : 'loadvol',
                  'vol_ticket'     : vol_ticket,
                  'drive_id'       : drive
                  }
        rt = self.send(ticket, 300, 10)
        if rt['status'][0] != e_errors.OK:
            Trace.log(e_errors.ERROR, "loadvol %s" % (rt['status'],))
        return rt

    def unloadvol(self, vol_ticket, mover, drive):
        ticket = {'work'           : 'unloadvol',
                  'vol_ticket' : vol_ticket,
                  'drive_id'       : drive
                  }
        rt = self.send(ticket,300,10)
        if rt['status'][0] != e_errors.OK:
            Trace.log(e_errors.ERROR, "unloadvol %s" % (rt['status'],))
        return rt

    def viewvol(self, volume, m_type):
        ticket = {'work' : 'viewvol',
                  'external_label' : volume,
                  'media_type' : m_type
                     }
        rt = self.send(ticket)
        return rt

    def list_volumes(self):
        host, port, listen_socket = callback.get_callback()
        listen_socket.listen(4)
        
        ticket = {'work' : 'list_volumes',
                  'callback_addr'  : (host,port)
                  }
        rt = self.send(ticket)
        if not e_errors.is_ok(rt):
            print "ERROR", rt
            return rt
        
        r, w, x = select.select([listen_socket], [], [], 15)
        if not r:
            reply = {'status' : (e_errors.TIMEDOUT,
                         "timeout waiting for media changer callback")}
            return reply
        control_socket, address = listen_socket.accept()
        
        try:
            d = callback.read_tcp_obj(control_socket)
        except e_errors.TCP_EXCEPTION:
            d = {'status':(e_errors.TCP_EXCEPTION, e_errors.TCP_EXCEPTION)}
        listen_socket.close()
        control_socket.close()
        return d
        
    def viewdrive(self, drive):
        ticket = {'work' : 'viewdrive',
                  'drive' : drive,
                  }
        rt = self.send(ticket)
        return rt

    def list_drives(self):
        ticket = {'work' : 'list_drives',
                  }
        rt = self.send(ticket)
        return rt

    def robotQuery(self):
        ticket = {'work' : 'robotQuery', }
        rt = self.send(ticket)
        return rt

    def list_slots(self):
        ticket = {'work' : 'list_slots',
                  }
        rt = self.send(ticket)
        return rt

    def doCleaningCycle(self, moverConfig):
        ticket = {'work'       : 'doCleaningCycle',
                  'moverConfig': moverConfig,
                  }
        rt = self.send(ticket,300,10)
        return rt

    def insertvol(self, IOarea, inNewLib):
        ticket = {'work'         : 'insertvol',
              'IOarea_name'  : IOarea,
          'newlib'       : inNewLib
         }
        if type(IOarea) != types.ListType:
            Trace.log(e_errors.ERROR, "ERROR:insertvol IOarea must be a list")
            rt = {'status':(e_errors.WRONGPARAMETER, 1, "IOarea must be a list")}
            return rt
        zz = raw_input('Insert volumes into I/O area. Do not mix media types.\nWhen I/O door is closed hit return:')
        if zz == "FakeOpenIODoor":
            ticket["FakeIOOpen"] = 'yes'
        rt = self.send(ticket,300,10)
        return rt

    def ejectvol(self, media_type, volumeList):
        ticket = {'work'         : 'ejectvol',
              'volList'      : volumeList,
              'media_type'   : media_type
                  }
        if type(volumeList) != types.ListType:
            Trace.log(e_errors.ERROR, "ERROR:ejectvol volumeList must be a list")
            rt = {'status':(e_errors.WRONGPARAMETER, 1, "volumeList must be a list")}
            return rt
        rt = self.send(ticket,300,10)
        return rt

    def set_max_work(self, max_work):
        ticket = {'work'           : 'set_max_work',
                  'max_work'        : max_work
                 }
        return self.send(ticket)

    def GetWork(self):
        ticket = {'work'           : 'getwork'
                 }
        return self.send(ticket)


class MediaChangerClientInterface(generic_client.GenericClientInterface):
    def __init__(self, args=sys.argv, user_mode=1):
        #self.do_parse = flag
        #self.restricted_opts = opts
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
        self.media_changer = ""
        self.get_work=0
        self.max_work=-1
        self.volume = 0
        self._import = 0
        self._export = 0
        self.mount = 0
        self.dismount = 0
        self.viewattrib = 0
        self.drive = 0
        self.show = 0
        self.show_drive = 0
        self.show_robot = 0
        self.show_volume = 0
        self.list_drives = 0
        self.list_volumes = 0
        self.list_slots = 0
        self.list = 0
        generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)

    def valid_dictionaries(self):
        return (self.alive_options, self.help_options, self.trace_options,
                self.media_options)

    media_options = {
        option.DISMOUNT:{option.HELP_STRING:"",
                      option.DEFAULT_VALUE:option.DEFAULT,
                      option.DEFAULT_TYPE:option.INTEGER,
                      option.VALUE_NAME:"volume",
                      option.VALUE_TYPE:option.STRING,
                      option.VALUE_USAGE:option.REQUIRED,
                      option.VALUE_LABEL:"external_label",
                      option.USER_LEVEL:option.ADMIN,
                      option.FORCE_SET_DEFAULT:option.FORCE,
                      option.EXTRA_VALUES:[{option.VALUE_USAGE:option.REQUIRED,
                                            option.VALUE_NAME:"drive",
                                            option.VALUE_TYPE:option.STRING}],
                      },
        option.GET_WORK:{option.HELP_STRING:"",
                         option.DEFAULT_VALUE:option.DEFAULT,
                         option.DEFAULT_TYPE:option.INTEGER,
                         option.VALUE_USAGE:option.IGNORED,
                         option.USER_LEVEL:option.ADMIN},
        option.LIST_DRIVES:{option.HELP_STRING:"List all drives.",
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.VALUE_USAGE:option.IGNORED,
                     option.USER_LEVEL:option.ADMIN},
        option.LIST:{option.HELP_STRING: "list all media changers in "
                     "configuration",
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.VALUE_USAGE:option.IGNORED,
                     option.USER_LEVEL:option.ADMIN},
         option.LIST_VOLUMES:{option.HELP_STRING:"List all volumes.",
                              option.DEFAULT_VALUE:option.DEFAULT,
                              option.DEFAULT_TYPE:option.INTEGER,
                              option.VALUE_USAGE:option.IGNORED,
                              option.USER_LEVEL:option.ADMIN},
        option.LIST_SLOTS:{option.HELP_STRING:"List all slot counts.",
                              option.DEFAULT_VALUE:option.DEFAULT,
                              option.DEFAULT_TYPE:option.INTEGER,
                              option.VALUE_USAGE:option.IGNORED,
                              option.USER_LEVEL:option.ADMIN},
        option.MAX_WORK:{option.HELP_STRING:"",
                         option.VALUE_TYPE:option.INTEGER,
                         option.VALUE_USAGE:option.REQUIRED,
                         option.USER_LEVEL:option.ADMIN},
        option.MOUNT:{option.HELP_STRING:"",
                      option.DEFAULT_VALUE:option.DEFAULT,
                      option.DEFAULT_TYPE:option.INTEGER,
                      option.VALUE_NAME:"volume",
                      option.VALUE_TYPE:option.STRING,
                      option.VALUE_USAGE:option.REQUIRED,
                      option.VALUE_LABEL:"external_label",
                      option.USER_LEVEL:option.ADMIN,
                      option.FORCE_SET_DEFAULT:option.FORCE,
                      option.EXTRA_VALUES:[{option.VALUE_USAGE:option.REQUIRED,
                                            option.VALUE_NAME:"drive",
                                            option.VALUE_TYPE:option.STRING}],
                      },
        option.SHOW:{option.HELP_STRING:"",
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.DEFAULT_NAME:"show",
                     option.VALUE_USAGE:option.IGNORED,
                     option.USER_LEVEL:option.ADMIN},
        option.SHOW_DRIVE:{option.HELP_STRING:"",
                           option.DEFAULT_VALUE:option.DEFAULT,
                           option.DEFAULT_TYPE:option.INTEGER,
                           option.VALUE_USAGE:option.IGNORED,
                           option.USER_LEVEL:option.ADMIN,
                           option.VALUE_NAME:"drive",
                           option.VALUE_TYPE:option.STRING,
                           option.VALUE_USAGE:option.REQUIRED,
                           option.FORCE_SET_DEFAULT:option.FORCE,
                           },
        option.SHOW_ROBOT:{option.HELP_STRING:"",
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.VALUE_USAGE:option.IGNORED,
                     option.USER_LEVEL:option.ADMIN},
        option.SHOW_VOLUME:{option.HELP_STRING:"Returns information about "
                            "a volume.",
                            option.DEFAULT_VALUE:option.DEFAULT,
                            option.DEFAULT_TYPE:option.INTEGER,
                            option.USER_LEVEL:option.ADMIN,
                            option.VALUE_NAME:"volume",
                            option.VALUE_TYPE:option.STRING,
                            option.VALUE_USAGE:option.REQUIRED,
                            option.FORCE_SET_DEFAULT:option.FORCE,
                            option.EXTRA_VALUES:[{option.VALUE_USAGE:option.REQUIRED,
                                                  option.VALUE_NAME:"media_type",
                                                  option.VALUE_TYPE:option.STRING}],
                       },
        }

    def parse_options(self):
        generic_client.GenericClientInterface.parse_options(self)

        if (getattr(self, "help", 0) or getattr(self, "usage", 0)):
            pass
        elif len(self.argv) <= 1: #if only "enstore media" is specified.
            self.print_help()
        elif self.list:
            self.print_media_changers()
        elif len(self.args) < 1: #if a valid switch doesn't have the MC.
            self.print_usage("expected media changer parameter")
        else:
            try:
                self.media_changer = self.args[0]
                del self.args[0]
            except KeyError:
                self.media_changer = ""

        self.media_changer = self.complete_server_name(self.media_changer,
                                                       "media_changer")

    def print_media_changers(self):
        config_host = enstore_functions2.default_host()
        config_port = enstore_functions2.default_port()
        csc = configuration_client.ConfigurationClient((config_host,
                                                        config_port))
        csc.dump_and_save()
        msg_spec = "%25s %15s %20s"
        print msg_spec % ("media changer", "host", "type")
        mc_dict = csc.get_media_changers(timeout=5, retry=3)
        for mc_name in mc_dict.values():
            mc_info = csc.get(mc_name['name'])
            print msg_spec % (mc_name['name'], mc_info['host'],
                              mc_info['type'])
            
        sys.exit(0)

def do_work(intf):
    # get a media changer client
    mcc = MediaChangerClient((intf.config_host, intf.config_port),
                             intf.media_changer)

    Trace.init(mcc.get_name(mcc.log_name))
    ticket = mcc.handle_generic_commands(intf.media_changer, intf)

    if ticket:
        pass

    elif intf.mount:
        vcc = volume_clerk_client.VolumeClerkClient(mcc.csc)
        vol_ticket = vcc.inquire_vol(intf.volume)
        ticket = mcc.loadvol(vol_ticket, intf.drive, intf.drive)
        del vcc
    elif intf.dismount:
        vcc = volume_clerk_client.VolumeClerkClient(mcc.csc)
        vol_ticket = vcc.inquire_vol(intf.volume)
        ticket = mcc.unloadvol(vol_ticket, intf.drive, intf.drive)
        del vcc
    elif intf._import:
        ticket=mcc.insertvol(intf.ioarea, intf.insertNewLib)
    elif intf._export:
        ticket=mcc.ejectvol(intf.media_type, intf.volumeList)
    elif intf.max_work  >= 0:
        ticket=mcc.set_max_work(intf.max_work)
    elif intf.get_work:
        ticket=mcc.GetWork()
        pprint.pprint(ticket)
    elif intf.show or intf.show_robot:
        ticket = mcc.robotQuery()
        tod = time.strftime("%Y-%m-%d:%H:%M:%S",time.localtime(time.time()))
        try:
            stat = ticket['status']
            delta_t = string.split(stat[2])[-1:][0]
            print tod, delta_t, stat
        except:
            print tod, -999, ticket
    elif intf.show_volume:
        t0 = time.time()
        ticket = mcc.viewvol(intf.volume, intf.media_type)
        if e_errors.is_ok(ticket):
            print "%17s %10s %20s %20s" % ("volume", "type",
                                           "state", "location")
            print "%17s %10s %20s %20s" % (intf.volume, ticket['media_type'],
                                           ticket['status'][3], "")
    elif intf.show_drive:
        t0 = time.time()
        ticket = mcc.viewdrive(intf.drive)
        if e_errors.is_ok(ticket) and ticket.get("drive_info", None):
            drive_info = ticket['drive_info']
            print "%12s %15s %15s %15s %8s" % ("name", "state", "status",
                                               "type", "volume")
            print "%12s %15s %15s %15s %8s" % \
                  (intf.drive, drive_info['state'],
                   drive_info.get("status", ""), drive_info['type'],
                   drive_info['volume'])
    elif intf.list_drives:
        ticket = mcc.list_drives()
        if e_errors.is_ok(ticket) and ticket.get("drive_list", None):
            print "%12s %15s %15s %15s %8s" % ("name", "state", "status",
                                              "type", "volume")
            for drive in ticket['drive_list']:
                print "%12s %15s %15s %15s %8s" % \
                      (drive['name'], drive['state'], drive.get("status", ""),
                       drive['type'], drive['volume'])
    elif intf.list_slots:
        ticket = mcc.list_slots()
        if e_errors.is_ok(ticket) and ticket.get("slot_list", None):
            print "%12s %12s %10s %10s %10s %10s" % ("location", "media type",
                                                     "total", "free", "used",
                                                     "disabled")
            for slot_info in ticket['slot_list']:
                print "%12s %12s %10s %10s %10s %10s" % \
                      (slot_info['location'], slot_info['media_type'],
                       slot_info['total'], slot_info['free'],
                       slot_info['used'], slot_info['disabled'])
    elif intf.list_volumes:
        ticket = mcc.list_volumes()
        if e_errors.is_ok(ticket) and ticket.get("volume_list", None):
            print "%17s %10s %20s %20s" % ("volume", "type", "state", "location")
            for volume in ticket['volume_list']:
                print "%17s %10s %20s %20s" % (volume['volume'], volume['type'],
                                         volume['state'], volume['location'])
    else:
        intf.print_help()
        sys.exit(0)

    del mcc.csc.u
    del mcc.u       # del now, otherwise get name exception (just for python v1.5???)

    mcc.check_ticket(ticket)

if __name__ == "__main__" :
    Trace.init("MEDCH CLI")
    Trace.trace(6,"mcc called with args "+repr(sys.argv))

    # fill in the interface
    intf = MediaChangerClientInterface(user_mode=0)
    do_work(intf)
