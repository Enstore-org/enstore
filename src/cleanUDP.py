#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

"""
    The purpose of this module is to provide a clean datagram
    interface for Enstore. By "clean" we mean that we try to
    provide a uniform interface on all platforms by masking specific
    errors.

    Specific errors that are masked:

    1) Linux ipv4 -- returning an error on the next UDP send or recieve
    when an ICMP port unreachable message is recieved. The socket 
    implementation will return, then automatically clear ECONNREFUSED.
    To handle this, we transparently retry self.retry_max times 

    cleanUDP.select() must be used instead of select.select()

"""

# system imports
import socket
import Trace
import errno
import time
import select
import os
import sys

# enstore imports
import e_errors

# Linux does not impliment a system wide UDP checksum (UDPCTL_CHECKSUM),
# but rather does it on a per socket basis.
if os.uname()[0] == "Linux":
    socket.SO_NO_CHECK = 11

# The following are from linux/in.h

IP_MTU_DISCOVER = 10             # MTU discover code
# IP_MTU_DISCOVER values
IP_PMTUDISC_DONT = 0             # Never send DF frames
IP_PMTUDISC_WANT = 1             # Use per route hints (default)
IP_PMTUDISC_DO = 2             # Always DF
IP_PMTUDISC_PROBE = 3             # Ignore dst pmtu


def Select(R, W, X, timeout):

    # we have an error under linux where we get an error, and
    # r and x are set, but there is no data. If the error is a spurious error,
    # we must delete the object from all lists.
    ##
    cleaned_r = []
    t0 = time.time()
    timeout = max(0.0, timeout)
    while 1:
        try:
            r, w, x = select.select(R, W, X, timeout)
        except select.error, msg:
            # If a signal interupts our select, try again.
            if msg.args[0] in [errno.EINTR]:
                time.sleep(1)
                continue
            else:
                raise select.error, msg  # all other errors

        time_elapsed = time.time() - t0
        remaining_timeout = max(0.0, timeout - time_elapsed)

        if r == cleaned_r:
            # If the timeout specified hasn't run out and
            # we don't have a ready socket keep trying.
            if r == w == x == [] and remaining_timeout > 0.0:
                continue

            # all except FD's as the same as not scrubbed
            # previously.
            return r, w, x, remaining_timeout
        cleaned_r = []
        for obj in r:
            try:
                if obj.scrub():
                    cleaned_r.append(obj)
            except:
                #Trace.trace( 6, "non clean UDP object" )
                cleaned_r.append(obj)


class cleanUDP:

    retry_max = 10
    previous_sendto_address = "N/A"
    this_sendto_address = "N/A"

    def __init__(self, protocol, kind):
        if kind != socket.SOCK_DGRAM:
            raise socket.error(errno.EINVAL, "expected SOCK_DGRAM")
        self.socket = socket.socket(protocol, kind)
        if os.uname()[0] == "Linux":
            # Enable UDP checksums.    These should be on by default.
            # Force them on by setting this value to 0.    (Note
            # the name is SO_NO_CHECK the NO means that to disable
            # checksums you pass setsockopt 1).
            #
            # To see how to do this for non-Linux machines
            # see page 498-499 of Unix Network Programming Volume
            # 1 Third Edition.
            # The main issue with using it here is that there
            # is not an interface to the sysctl() system call
            # from python.
            try:
                self.socket.setsockopt(socket.SOL_SOCKET,
                                       socket.SO_NO_CHECK, 0)
                is_udp_checksum_off = self.socket.getsockopt(
                    socket.SOL_SOCKET, socket.SO_NO_CHECK)
                if is_udp_checksum_off:
                    sys.stderr.write(
                        "UDP checksum not enabled.\n")
            except socket.error:
                pass
            # Allow UDP packet fragmentation.
            # It is disallowed by default.
            # If it is disallowed then the following problem occurs.
            # If UDP packet size is less than MTU on a sender node
            # and bigger than MTU on receiver node the packet gets
            # delivered without fragmentation and rejected
            # on receiver node because it can not treat frames bigger than MTU
            try:
                self.socket.setsockopt(
                    socket.SOL_IP, IP_MTU_DISCOVER, IP_PMTUDISC_DONT)
                rc = self.socket.getsockopt(socket.SOL_IP, IP_MTU_DISCOVER)
                if rc != IP_PMTUDISC_DONT:
                    sys.stderr.write(
                        "IP_MTU_DISCOVER is set to %s" % (rc,))
            except socket.error:
                pass

        return

    # def __del__(self):
    #    self.socket.close()

    def scrub(self):
        self.socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
        r, w, x = select.select([self], [], [self], 0)
        if r:
            return 1  # it's clean - there really is a read to do
        # it went away - just the icmp message under A. Cox's
        # linux implementation
        return 0

    def __getattr__(self, attr):
        return getattr(self.socket, attr)

    # Mitigate case 1 -- ECONNREFUSED from previous sendto
    def recvfrom(self, bufsize, rcv_timeout=10):
        data = ("", ("", 0))
        for n in range(self.retry_max):
            try:
                r, _, _ = select.select([self.socket], [], [], rcv_timeout)
                if r:
                    data = self.socket.recvfrom(bufsize)
                    return data
            except socket.error:
                self.logerror("recvfrom", n)
        return data

    # Mitigate case 1 -- ECONNREFUSED from previous sendto
    def sendto(self, data, address):
        if not address:
            return 0

        self.previous_sendto_address = self.this_sendto_address
        self.this_sendto_address = address

        for n in range(self.retry_max):
            try:
                return self.socket.sendto(data, address)
            except (socket.error, socket.gaierror, socket.herror,
                    select.error), msg:
                e_errno = getattr(msg, 'errno', msg.args[0])
                if e_errno in [errno.EMSGSIZE]:
                    """
                    #These error are considered fatal,
                    # where retrying will always fail too.
                    message = "sendto %s: %s: data length %s" % \
                            (str(msg), address, len(data))

                    Trace.log(e_errors.ERROR, message)
                    #Log the stack trace so we know
                    # what request was being processed.
                    Trace.handle_error(sys.exc_info()[0],
                                 sys.exc_info()[1],
                                 sys.exc_info()[2])
                    """
                    # A long message can now be
                    # handled by generic_client and
                    # dispatching_worker.    Don't log a
                    # traceback here.

                    # Re-raise here since with this error
                    # retrying will never succeed.
                    raise sys.exc_info()[0], \
                        sys.exc_info()[1], \
                        sys.exc_info()[2]
                elif e_errno in [socket.EAI_NONAME]:
                    # message = "sendto %s: %s" % \
                    #        (str(msg), address)
                    #Trace.log(e_errors.ERROR, message)

                    # Re-raise here since with this error
                    # retrying will never succeed.
                    # Inject the addess into the error
                    # string so the users see the address
                    # that is causing the error.
                    raise sys.exc_info()[0], \
                        (socket.EAI_NONAME,
                            "%s: %s" % (str(msg),
                                        str(address))
                         ), \
                        sys.exc_info()[2]
                else:
                    Trace.log(e_errors.ERROR,
                              str(sys.exc_info()[0]),
                              str(sys.exc_info()[1]))
                    self.logerror("sendto", n)

        try:
            return self.socket.sendto(data, address)
        except (socket.error), msg:
            if msg.args[0] in [errno.EBADF, errno.EBADFD]:
                raise socket.error, \
                    (msg.args[0], "%s: %s" % (msg.args[1], self.socket.fileno())), \
                    sys.exc_info()[2]
            else:
                raise socket.error, \
                    (msg.args[0], "%s: %s" % (msg.args[1], address)), \
                    sys.exc_info()[2]

    def logerror(self, sendto_or_recvfrom, try_number):
        badsockerrno = self.socket.getsockopt(
            socket.SOL_SOCKET, socket.SO_ERROR)
        try:
            badsocktext = repr(errno.errorcode[badsockerrno])
        except:
            badsocktext = repr(badsockerrno)
        etext = "cleanUDP %s try %d %s failed on %s last %s" % (
                sendto_or_recvfrom, try_number,
                badsocktext, self.this_sendto_address,
                self.previous_sendto_address)

        Trace.log(e_errors.ERROR, etext)


if __name__ == "__main__":
    sout = cleanUDP(socket.AF_INET, socket.SOCK_DGRAM)
    sout.bind(('localhost', 303030))
    # on linux, should see one retry from the following.

    sout.sendto("all dogs have fleas", ('localhost', 303031))
    r, w, x = select.select([sout], [sout], [sout], 1.0)
    if not x and not r and w:
        print "expected select.select behavoir on non-linux " \
              "and post 2.4 linux kernel"
    elif x and r and w:
        print "expected select.select behavior on linux, " \
              "pre 2.2 kernel"
    elif not x and r and w:
        print "expected select.select behavior on linux, " \
              "post 2.2 kernel"
    else:
        print "***unexpected    behavior on _any_ platform"
    r, w, x, remaining_time = Select([sout], [sout], [sout], 1.0)

    if not r and not x:
        print "expected behavior"
    else:
        print "***unexpected behavior"

    sout.sendto("all dogs have fleas", ('localhost', 303031))
    sin = cleanUDP(socket.AF_INET, socket.SOCK_DGRAM)
    sin.bind(('localhost', 303031))
    sout.sendto("Expected behavior", ('localhost', 303031))
    print sin.recvfrom(1000)
