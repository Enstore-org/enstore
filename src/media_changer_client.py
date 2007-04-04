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

#enstore imports
#import udp_client
import option
import generic_client
import Trace
import volume_clerk_client
import e_errors

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
        self.show_drive = 0
        self.show_robot = 0
        self.show_volume = 0
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
        option.LIST_DRIVES:{option.HELP_STRING:"",
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
        option.SHOW:{option.HELP_STRING:"alias for --show-robot",
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.DEFAULT_NAME:"show-robot",
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
        option.SHOW_VOLUME:{option.HELP_STRING:"",
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
    elif intf.show_robot:
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
        delta_t = time.time() - t0
        tod = time.strftime("%Y-%m-%d:%H:%M:%S",time.localtime(time.time()))
        try:
            stat = ticket['status']
            #delta_t = string.split(stat[2])[-1:][0]
            print tod, delta_t, stat
        except:
            print tod, -999, ticket
    elif intf.show_drive:
        t0 = time.time()
        ticket = mcc.viewdrive(intf.drive)
        delta_t = time.time() - t0
        tod = time.strftime("%Y-%m-%d:%H:%M:%S",time.localtime(time.time()))
        try:
            stat = ticket['status']
            #delta_t = string.split(stat[2])[-1:][0]
            print tod, delta_t, stat
        except:
            print tod, -999, ticket
    elif intf.list_drives:
        ticket = mcc.list_drives()
        if e_errors.is_ok(ticket) and ticket.get("drive_list", None):
            print "%12s %15s %15s %15s %8s" % ("name", "state", "status",
                                              "type", "volume")
            for drive in ticket['drive_list']:
                print "%12s %15s %15s %15s %8s" % \
                      (drive['name'], drive['state'], drive.get("status", ""),
                       drive['type'], drive['volume'])
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
