import sys
import os
from SocketServer import *
from configuration_client import *
from volume_clerk_client import VolumeClerkClient
from callback import *
from dispatching_worker import DispatchingWorker
from generic_server import GenericServer
from udp_client import UDPClient

# Read write work lists
# set up and manipulate the list of requests to read or write
# along with their priorities

pending_work = []       # list of read write work tickets

# here is where we setup priority for work that needs to get done
def priority(ticket) :
    if ticket["work"] == "write_to_hsm" :
        return 10
    return 1


# insert work into our queue based on its priority
def queue_pending_work(ticket) :
    ticket["priority"] = priority(ticket)
    i = 0
    tryp = ticket["priority"]
    for item in pending_work :
        if tryp > item["priority"] :
            break
        i = i + 1
    pending_work.insert(i, ticket)


##############################################################

work_at_movers = []
work_awaiting_bind = []


# return a list of busy volumes for a given file family
def busy_vols_in_family (family_name):
    vols = []
    for w in work_at_movers + work_awaiting_bind :
        if w["file_family"] == family_name :
            vols.append(w["external_label"])
    return vols


# check if a particular volume with given label is busy
def is_volume_busy(external_label) :
    for w in work_at_movers + work_awaiting_bind :
        if w["external_label"] == external_label :
            return 1
    return 0


# return ticket for given labelled volume in bind queue
def get_awaiting_work(external_label) :
    for w in work_awaiting_bind :
        if w["external_label"] == external_label :
            return w
    return {}


# return ticket if given labelled volume in mover queue
def get_work_at_movers(external_label) :
    for w in work_at_movers :
        if w["external_label"] == external_label :
            return w
    return {}


##############################################################

# is there any work for any volume?
def next_work_any_volume(csc) :

    # look in pending work queue for reading or writing work
    for w in pending_work:

        # if we need to read and volume is busy, check later
        if w["work"] == "read_from_hsm" :
            if is_volume_busy(w["external_label"])  :
                continue
            # otherwise we have found a volume that has read work pending
            return w

        # if we need to write: ask the volume clerk for a volume, but first go
        # find volumes we _dont_ want to hear about -- that is volumes in the
        # apropriate family which are currently at movers.
        elif w["work"] == "write_to_hsm" :
            vol_veto_list = busy_vols_in_family(w["file_family"])
            # only so many volumes can be written to at one time
            if len(vol_veto_list) >= w["file_family_width"] :
                continue
            # width not exceeded, ask volume clerk for a new volume.
            vc = VolumeClerkClient(csc)
            v = vc.next_write_volume (w["library"],
                                      w["size_bytes"],
                                      w["file_family"],
                                      vol_veto_list)

            # If the volume clerk has no volumes and our veto list was empty,
            # then we have run out of space for this file family == error
            if (len(vol_veto_list) == 0 and v["status"] != "ok") :
                w["status"] = v["status"]
                return w
            # found a volume that has write work pending - return it
            w["external_label"] = v["external_label"]
            return w

        # alas, all I know about is reading and writing
        else :
            raise "assertion error"

    # if the pending work queue is empty, then we're done
    return {"status" : "nowork"}


# is there any work for this volume??  v is a work ticket with info
def next_work_this_volume(v) :

    # look in pending work queue for reading or writing work
    for w in pending_work:

        # writing to this volume?
        if (w["work"]           == "write_to_hsm"    and
            w["file_family"]    == v["file_family"]  and
            w["size_bytes"]    <= v["remaining_bytes"]) :
            w["external_label"] = v["external_label"]
            # ok passed criteria, return write work ticket
            return w

        # reading from this volume?
        elif (w["work"]           == "read_from_hsm" and
              w["external_label"] == v["external_label"] ) :
            # ok passed criteria, return read work ticket
            return w

        # alas, all I know about is reading and writing
        else :
            raise "assertion error"

    # if the pending work queue is empty, then we're done
    return {"status" : "nowork"}

##############################################################


# methods that can be inherited by any library manager
class LibraryManagerMethods(DispatchingWorker) :

    def write_to_hsm(self, ticket):
        ticket["status"] = "ok"
        self.reply_to_caller(ticket) # reply now to avoid deadlocks
        queue_pending_work(ticket)


    def read_from_hsm(self, ticket):
        ticket["status"] = "ok"
        self.reply_to_caller(ticket)
        queue_pending_work(ticket)


    # mover is idle - see what we can do
    def idle_mover(self, mticket) :
        # check our schedule
        w = self.schedule()

        # no work means we're done
        if w["status"] == "nowork":
            self.reply_to_caller({"work" : "nowork"})

        # ok, we have some work - bind the volume
        elif w["status"] == "ok" :
            # reply now to avoid deadlocks
            self.reply_to_caller({"work"           : "bind_volume",
                                  "external_label" : w["external_label"] })
            # put it into our bind queue and take it out of pending queue
            work_awaiting_bind.append(w)
            pending_work.remove(w)

        # alas
        else :
            raise "assert error"

    # we have a volume already bound - any more work??
    def have_bound_volume(self, mticket) :
        # just did some work, delete it from queue
        w = get_work_at_movers (mticket["external_label"])
        if w:
            work_at_movers.remove(w)

        # if we have work awaiting the bind, pass that work and delete it
        # from the list and  return
        w = get_awaiting_work(mticket["external_label"])
        if w :
            self.reply_to_caller(w) # reply now to avoid deadlocks
            work_awaiting_bind.remove(w)
            work_at_movers.append(w)
            return

        # otherwise, see if this volume will do for any other work pending
        w = next_work_this_volume(mticket)
        if w["status"] == "ok" :
            self.reply_to_caller(w) # reply now to avoid deadlocks
            pending_work.remove(w)
            work_at_movers.append(w)


        # if the pending work queue is empty, then we're done
        elif  w["status"] == "nowork" :
            self.reply_to_caller({"work" : "unbind_volume"})

        # alas
        else:
            raise "assertion error"


    # if the work is on the awaiting bind list, it is the library manager's
    #  responsibility to retry
    # THE LIBRARY COULD NOT MOUNT THE TAPE IN THE DRIVE AND IF THE MOVER
    # THOUGHT THE VOLUME WAS POISONED, IT WOULD TELL THE VOLUME CLERK.
    def unilateral_unbind(self, ticket) :
        # get the work ticket for the volume
        w = get_awaiting_work(ticket["external_label"])
        if w:
            work_awaiting_bind.remove(w)
            queue_pending_work(w)

        # else, it is the user's responsibility to retry
        w = get_work_at_movers (ticket["external_label"])
        if w:
            work_awaiting_movers.remove(w)

        self.reply_to_caller({"work" : "nowork"})


    # what is next on our list of work?
    def schedule(self) :
        while 1 :
            w = next_work_any_volume(self.csc)
            if w["status"] == "ok" or w["status"] == "nowork" :
                return w
            # some sort of error, like write
            # work and no volume available
            # so bounce. status is already bad...
            pending_work.remove(w)
            send_to_user_callback(w)

    # what is going on
    def printwork(self,ticket) :
        ticket["status"] = "ok"
        rticket = {}
        rticket["status"] = "ok"
        rticket["at movers"] = work_at_movers
        rticket["awaiting volume bind"] = work_awaiting_bind
        rticket["pending_work"] = pending_work
        self.reply_to_caller(rticket)

    # what is going on
    def getwork(self,ticket) :
        ticket["status"] = "ok"
        self.reply_to_caller(ticket) # reply now to avoid deadlocks
        self.get_user_sockets(ticket)
        rticket = {}
        rticket["status"] = "ok"
        rticket["at movers"] = work_at_movers
        rticket["awaiting volume bind"] = work_awaiting_bind
        rticket["pending_work"] = pending_work
        rticket["ticket"] = repr(ticket)
        self.data_socket.send(repr(rticket))
        self.data_socket.close()
        self.control_socket.send(dict_to_a(ticket))
        self.control_socket.close()


    # get a port for the data transfer
    # tell the user I'm your mover and here's your ticket
    def get_user_sockets(self, ticket) :
        library_manager_host,library_manager_port,listen_socket = get_callback()
        listen_socket.listen(4)
        ticket["library_manager_callback_host"] = library_manager_host
        ticket["library_manager_callback_port"] = library_manager_port
        self.control_socket = user_callback_socket(ticket)

        # we expect a prompt call-back here, and should protect
        # against users not getting back to us. The best protection
        # would be to kick off if the user dropped the control_socket,
        # but I am at home and am not able to find documentation
        # on select...

        data_socket, address = listen_socket.accept()
        self.data_socket = data_socket
        listen_socket.close()


class LibraryManager(LibraryManagerMethods, GenericServer, UDPServer) :
    pass

if __name__ == "__main__" :
    import getopt
    import socket

    # defaults
    config_host = "localhost"
    #(config_host,ca,ci) = socket.gethostbyaddr(socket.gethostname())
    config_port = "7500"
    config_list = 0

    # see what the user has specified. bomb out if wrong options specified
    options = ["config_host=","config_port=","config_list","help"]
    optlist,args=getopt.getopt(sys.argv[1:],'',options)
    for (opt,value) in optlist :
        if opt == "--config_host" :
            config_host = value
        elif opt == "--config_port" :
            config_port = value
        elif opt == "--config_list" :
            config_list = 1
        elif opt == "--help" :
            print "python ",sys.argv[0], options, "library"
            print "   do not forget the '--' in front of each option"
            sys.exit(0)

    # bomb out if can't translate host
    ip = socket.gethostbyname(config_host)

    # bomb out if port isn't numeric
    config_port = string.atoi(config_port)

    # bomb out if we don't have a library
    if len(args) < 1 :
        print "python",sys.argv[0], options, "library"
        print "   do not forget the '--' in front of each option"
        sys.exit(1)

    if config_list :
        print "Connecting to configuration server at ",config_host,config_port
    csc = configuration_client(config_host,config_port)

    keys = csc.get(args[0] + ".library_manager")

    #  set ourself up on that port and start serving
    methods =  LibraryManagerMethods()
    lm =  LibraryManager( (keys['host'], keys['port']), methods)
    lm.set_csc(csc)
    lm.serve_forever()
