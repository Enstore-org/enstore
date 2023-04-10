#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################
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
import socket
import select
import errno

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

FAKEOPENIODOOR = "FakeOpenIODoor" #used to test the robot

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

    ##These functions should really take a named parameter list rather than
    ## "vol_ticket".  It's not clear what keys need to be present in
    ## vol_ticket.  Looks like at least external_label and media_type are
    ## needed.
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

    def viewvol(self, volume, m_type, rcv_timeout = 0, rcv_tries = 0, reserve=False):
        ticket = {'work' : 'viewvol',
                  'external_label' : volume,
                  'media_type' : m_type,
                  'reserve': reserve,
                  }
        rt = self.send(ticket, rcv_timeout, rcv_tries)
        return rt


    def list_volumes(self, rcv_timeout = 0, rcv_tries = 0):
        host, port, listen_socket = callback.get_callback()
        listen_socket.listen(4)

        #We want to attempt to try the 'list_volumes2' protocol.  However,
        #only STK has been updated to use it.  AML2 will set this back to
        # "list_volumes" before sending back its reply.
        #12-16-2008: Go back to just list_volumes for all; make list_volumes2
        # obsolete.
        ticket = {
            #'work' : 'list_volumes2',
            'work' : 'list_volumes',
            'callback_addr'  : (host, port)
            }
        rt = self.send(ticket, rcv_timeout, rcv_tries)
        if not e_errors.is_ok(rt):
            #print "ERROR", rt
            return rt

        r, w, x = select.select([listen_socket], [], [], 15)
        if not r:
            reply = {'status' : (e_errors.TIMEDOUT,
                         "timeout waiting for media changer callback")}
            return reply
        control_socket, address = listen_socket.accept()

        if rt['work'] == "list_volumes":
            try:
                reply = self.__list_volumes(control_socket, rt)
            except:
                Trace.log(e_errors.UNKNOWN, str(sys.exc_info()[1]))
        elif rt['work'] == "list_volumes2":
            try:
                reply = self.__list_volumes2(control_socket, rt)
            except:
                Trace.log(e_errors.UNKNOWN, str(sys.exc_info()[1]))
        else:
            reply = {'status' : (e_errors.TIMEDOUT,
                                 "Invalid list_volumes protocol returned.")}

        listen_socket.close()
        control_socket.close()

        return reply

    def __list_volumes(self, control_socket, ticket):
        __pychecker__ = "unusednames=ticket" #Keep pychecker happy
        try:
            d = callback.read_tcp_obj(control_socket, 1800) # 30 min
        except (socket.error, select.error, e_errors.EnstoreError), msg:
            if msg.errno == errno.ETIMEDOUT:
                d = {'status':(e_errors.TIMEDOUT, self.server_name)}
            else:
                d = {'status':(e_errors.NET_ERROR, str(msg))}
        except e_errors.TCP_EXCEPTION:
            d = {'status':(e_errors.TCP_EXCEPTION, e_errors.TCP_EXCEPTION)}

        return d

    # OBSOLETE
    def __list_volumes2(self, control_socket, ticket):
        #Keep getting each volume's information one at a time.
        ticket['volume_list'] = []
        t = -1
        while t:
            try:
                t = callback.read_tcp_obj(control_socket)
                if t and type(t) == type(()) and not t[0]:
                    ticket['status'] = (e_errors.OK, None)
                    break #received sentinal

                #convert the tuple into a dictionary.
                d = {'volume' : t[0],
                     'state' : t[1],
                     'type' : t[2],
                     'location' : t[3],
                     }
                ticket['volume_list'].append(d)
            except (select.error, socket.error, e_errors.EnstoreError), msg:
                if msg.errno == errno.ETIMEDOUT:
                    ticket['status'] = (e_errors.TIMEDOUT, self.server_name)
                else:
                    ticket['status'] = (e_errors.NET_ERROR, str(msg))
                break
            except e_errors.TCP_EXCEPTION:
                ticket['status'] = (e_errors.TCP_EXCEPTION,
                                    e_errors.TCP_EXCEPTION)
                break
        else:
            ticket['status'] = (e_errors.OK, None)

        return ticket

    def list_clean(self, rcv_timeout = 0, rcv_tries = 0):
        host, port, listen_socket = callback.get_callback()
        listen_socket.listen(4)

        ticket = {'work' : 'list_clean',
                  'callback_addr'  : (host,port)
                  }
        rt = self.send(ticket, rcv_timeout, rcv_tries)
        if not e_errors.is_ok(rt):
            return rt

        r, w, x = select.select([listen_socket], [], [], 15)
        if not r:
            reply = {'status' : (e_errors.TIMEDOUT,
                         "timeout waiting for media changer callback")}
            return reply
        control_socket, address = listen_socket.accept()

        try:
            d = callback.read_tcp_obj(control_socket)
        except (select.error, socket.error, e_errors.EnstoreError), msg:
            if msg.errno == errno.ETIMEDOUT:
                d = {'status':(e_errors.TIMEDOUT, self.server_name)}
            else:
                d = {'status':(e_errors.NET_ERROR, str(msg))}
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

    def displaydrive(self, drive):
        ticket = {'work' : 'displaydrive',
                  'drive' : drive,
                  }
        rt = self.send(ticket)
        return rt

    def list_drives(self, rcv_timeout = 0, rcv_tries = 0):
        ticket = {'work' : 'list_drives',
                  }
        rt = self.send(ticket, rcv_timeout, rcv_tries)
        return rt

    def robotQuery(self):
        ticket = {'work' : 'robotQuery', }
        rt = self.send(ticket)
        return rt

    def list_slots(self, rcv_timeout = 0, rcv_tries = 0):
        ticket = {'work' : 'list_slots',
                  }
        rt = self.send(ticket, rcv_timeout, rcv_tries)
        return rt

    def doCleaningCycle(self, moverConfig):
        ticket = {'work'       : 'doCleaningCycle',
                  'moverConfig': moverConfig,
                  }
        rt = self.send(ticket,300,10)
        return rt

    def insertvol(self, external_label = None):
        ticket = {'work'         : 'insertvol',
                  'external_label'  : external_label,
         }
        rt = self.send(ticket,300,10)
        return rt

    def ejectvol(self, external_label = None):
        ticket = {'work'         : 'ejectvol',
                  'volList'      : [external_label],
                  }
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

    def update_db(self, volume_address, volume_name,  drive_address, volume_name_in_drive):
        ticket = {'work'           : 'update_db',
                  'volume': {'address': volume_address,
                             'volume':  volume_name,
                             },
                  'drive': {'address': drive_address,
                            'volume': volume_name_in_drive,
                            }
                 }
        if all (v == None for v in (volume_address, volume_name,  drive_address, volume_name_in_drive)):
            # on the server side there will be inventory running for quite a while
            to = 300
            retry = 3
        else:
            # the server will return the reply pretty fast
            to = RCV_TIMEOUT
            retry = RCV_TRIES
        return self.send(ticket, to, retry)

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
        self.insert = 0 #from robot, not drive
        self.eject = 0  #from robot, not drive
        self.ioarea = 0 #used with --eject
        self.remove = 0 #used with --eject
        self.mount = 0
        self.dismount = 0
        self.viewattrib = 0
        self.drive = 0
        self.show = 0
        self.show_drive = 0
        self.display = 0
        self.show_robot = 0
        self.show_volume = 0
        self.list_drives = 0
        self.list_volumes = 0
        self.list_slots = 0
        self.list_clean = 0
        self.list = 0
        self.update = 0
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
        option.EJECT:{option.HELP_STRING:"Remove tapes from robot.",
                      option.DEFAULT_VALUE:option.DEFAULT,
                      option.DEFAULT_TYPE:option.INTEGER,
                      option.VALUE_LABEL:"volume_list",
                      option.VALUE_USAGE:option.IGNORED,
                      option.USER_LEVEL:option.ADMIN,
                      },
        option.GET_WORK:{option.HELP_STRING:"",
                         option.DEFAULT_VALUE:option.DEFAULT,
                         option.DEFAULT_TYPE:option.INTEGER,
                         option.VALUE_USAGE:option.IGNORED,
                         option.USER_LEVEL:option.ADMIN},
        option.INSERT:{option.HELP_STRING:"Insert tape into robot.",
                       option.DEFAULT_VALUE:option.DEFAULT,
                       option.DEFAULT_TYPE:option.INTEGER,
                       option.VALUE_LABEL:'[volume|"all"]',
                       option.VALUE_USAGE:option.IGNORED,
                       option.USER_LEVEL:option.ADMIN,
                       },
        option.IOAREA:{option.HELP_STRING:
                       "Used with --eject to specify IO area.",
                       option.DEFAULT_VALUE:option.DEFAULT,
                       option.DEFAULT_TYPE:option.STRING,
                       option.VALUE_USAGE:option.REQUIRED,
                       option.USER_LEVEL:option.ADMIN,
                       },
        option.LIST:{option.HELP_STRING: "list all media changers in "
                     "configuration",
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.VALUE_USAGE:option.IGNORED,
                     option.USER_LEVEL:option.ADMIN},
        option.LIST_CLEAN:{option.HELP_STRING:"List cleaning volumes.",
                              option.DEFAULT_VALUE:option.DEFAULT,
                              option.DEFAULT_TYPE:option.INTEGER,
                              option.VALUE_USAGE:option.IGNORED,
                              option.USER_LEVEL:option.ADMIN},
        option.LIST_DRIVES:{option.HELP_STRING:"List all drives.",
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.VALUE_USAGE:option.IGNORED,
                     option.USER_LEVEL:option.ADMIN},
        option.LIST_SLOTS:{option.HELP_STRING:"List all slot counts.",
                              option.DEFAULT_VALUE:option.DEFAULT,
                              option.DEFAULT_TYPE:option.INTEGER,
                              option.VALUE_USAGE:option.IGNORED,
                              option.USER_LEVEL:option.ADMIN},
        option.LIST_VOLUMES:{option.HELP_STRING:"List all volumes.",
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
         option.REMOVE:{option.HELP_STRING:
                        "Used with --eject to purge the volume info.",
                        option.DEFAULT_VALUE:option.DEFAULT,
                        option.DEFAULT_TYPE:option.INTEGER,
                        option.VALUE_USAGE:option.IGNORED,
                        option.USER_LEVEL:option.ADMIN},
        option.SHOW:{option.HELP_STRING:"",
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.DEFAULT_NAME:"show",
                     option.VALUE_USAGE:option.IGNORED,
                     option.USER_LEVEL:option.ADMIN},
        option.SHOW_DRIVE:{option.HELP_STRING:"Returns information about a drive",
                           option.DEFAULT_VALUE:option.DEFAULT,
                           option.DEFAULT_TYPE:option.INTEGER,
                           option.VALUE_USAGE:option.IGNORED,
                           option.USER_LEVEL:option.ADMIN,
                           option.VALUE_NAME:"drive",
                           option.VALUE_TYPE:option.STRING,
                           option.VALUE_USAGE:option.REQUIRED,
                           option.FORCE_SET_DEFAULT:option.FORCE,
                           },
        option.DISPLAY:{option.HELP_STRING:"",
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
          option.UPDATE:{option.HELP_STRING:
                       "update the status information. Use with caution as it causes inventory of the whole TS4500 library",
                       option.DEFAULT_TYPE:option.INTEGER,
                       option.DEFAULT_VALUE:option.DEFAULT,
                       option.VALUE_USAGE:option.IGNORED,
                       option.USER_LEVEL:option.ADMIN,
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
                self.media_changer = self.args[-1]
                del self.args[-1]
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
    elif intf.insert:
        print 'Updating DB before inserting. This may take about a minute'
        rt = mcc.update_db(None, None, None, None)
        if e_errors.is_ok(rt):
            try:
                vol = intf.args[0]
            except IndexError:
                vol = None
            if vol == 'all':
                while True:
                    ticket = mcc.insertvol(None)
                    print ticket['status']
                    if not e_errors.is_ok(ticket):
                        if ticket['status'][0] == e_errors.MC_QUEUE_FULL:
                            time.sleep(2)
                        else:
                            break
            else:
                ticket = mcc.insertvol(vol)
                print ticket['status']
        else:
            print rt['status']
    elif intf.eject:
        while True:
            ticket=mcc.ejectvol(intf.args[0])
            if e_errors.is_ok(ticket):
                break
            else:
                time.sleep(10)
        print ticket
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
        ticket = mcc.viewvol(intf.volume, intf.media_type)
        if e_errors.is_ok(ticket):
            location = ticket.get("location", "")
            phys_loc = ticket.get("phys_location", "")
            s_loc = '%s(%s)'%(location, phys_loc)
            print "%17s %10s %20s %20s" % ("volume", "type",
                                           "state", "location")
            print "%17s %10s %20s %20s" % (intf.volume, ticket['media_type'],
                                           ticket['status'][3],
                                           s_loc)
    elif intf.show_drive:
        ticket = mcc.viewdrive(intf.drive)
        if e_errors.is_ok(ticket) and ticket.get("drive_info", None):
            drive_info = ticket['drive_info']
            drive = drive_info.get('location', intf.drive)
            phys_loc = drive_info.get('phys_location')
            s = '%s'%(drive, )
            if phys_loc:
                s = '%s(%s)'%(drive, phys_loc)
            print "%16s %15s %15s %15s %8s" % ("name", "state", "status",
                                               "type", "volume")
            print "%16s %15s %15s %15s %8s" % \
                  (s, drive_info['state'],
                   drive_info.get("status", ""), drive_info['type'],
                   drive_info['volume'])
    elif intf.display:
        ticket = mcc.displaydrive(intf.drive)
        if e_errors.is_ok(ticket) and ticket.get("drive_info", None):
            drive_info = ticket['drive_info']
            print "%12s %15s" % ("name", 'Wwn')
            print "%12s %20s" % (drive_info['drive'], drive_info['Wwn'])
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
        if 'MTXN_MediaLoader' in ticket.get('MC_class', ''):
            print ticket['volume_list']
            return
        if e_errors.is_ok(ticket) and ticket.get("volume_list", None):
            if type(ticket['volume_list']) is str:
                print ticket['volume_list']
            else:
                print "%17s %10s %20s %20s" % ("volume", "type", "state", "location")
                for volume in ticket['volume_list']:
                        print "%17s %10s %20s %20s" % (
                        volume['volume'], 
                        volume['type'],
                        volume['state'], 
                        volume['location']
                        )
            return
    elif intf.list_clean:
        ticket = mcc.list_clean()
        if e_errors.is_ok(ticket) and ticket.get("clean_list", None):
            if 'MTXN_MediaLoader' in ticket.get('MC_class', ''):
                print ticket['clean_list']
                return
            print "%17s %10s %10s %10s %10s" % ("volume", "type", "max",
                                                "current", "remaining")
            for volume in ticket['clean_list']:
                print "%17s %10s %10s %10s %10s" % (volume['volume'],
                                                    volume['type'],
                                                    volume['max_usage'],
                                                    volume['current_usage'],
                                                    volume['remaining_usage'],
                                                    )
    elif intf.update:
        print 'This may take about a minute'
        ticket = mcc.update_db(None, None, None, None)
        print ticket['status']

    else:
        intf.print_help()
        sys.exit(0)

    del mcc.csc.u
    del mcc.u       # del now, otherwise get name exception (just for python v1.5???)

    mcc.check_ticket(ticket)

if __name__ == "__main__":   # pragma: no cover
    Trace.init("MEDCH CLI")
    Trace.trace(6,"mcc called with args "+repr(sys.argv))

    # fill in the interface
    intf = MediaChangerClientInterface(user_mode=0)
    do_work(intf)
