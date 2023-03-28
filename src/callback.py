#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

"""
The following functions are those intended for use by other modules:
read_obj:          Use for reading python objects from a pipe file descriptor.
read_tcp_obj:      Use for reading python objects from a tcp socket.
write_obj:         Use for writing python objects to a pipe file descriptor.
write_tcp_obj_new: Use for writing python objects to a tcp socket.
get_callback:      Use to obtain a tcp socket.

Use of write_tcp_obj() is discouraged for new code.  It uses repr instead
of pickle.
"""

# system imports
import time
import sys
import os
import random
import select
import socket
import cPickle
import errno
import fcntl
import types
import struct

# enstore imports
import Trace
import e_errors
import checksum
import host_config
from en_eval import en_eval
import Interfaces

MSG_LEN_POSITIONS = 12
MSG_LEN_POSITIONS_OLD = 8
PROTOCOL = "PROTO001"  # must be 8 characters to be used in place of old message length


def hex8(x):
    """
    Converts numeric argument to 8 byte hex string.
    Not intended for external use.

    Args:
        x (int): Number to convert to hex

    Returns:
        (string): Input in hex8 format (no preceding '0x')
    """
    s = hex(x)[2:]  # kill the 0x
    if isinstance(x, type(1L)):
        s = s[:-1]  # kill the L
    s_len = len(s)
    if s_len > 8:
        raise OverflowError(x)
    return '0' * (8 - s_len) + s


def get_socket_read_queue_length(sock):
    """
    Send FIONREAD on the socket to get the number of bytes to read.
    Not intended for external use. Called in a couple of binaries.

    Args:
        sock (Socket): Socket to read queue length of

    Returns:
        (int): Socket read queue length

    Raises:
        AttributeError (FIONREAD): Unable to determine FIONREAD opt code
    """
    OPT = getattr(fcntl, "FIONREAD", None)
    if OPT is None:
        if os.uname()[0] == "Linux":
            OPT = 0x541B  # FIONREAD on Linux.
        if os.uname()[0][:4] == "IRIX" or \
                os.uname()[0] == "SunOS" or \
                os.uname()[0] == "OSF1":
            OPT = 1074030207  # Pulled from header files.
    if OPT is not None:
        # Unpack socket return value into an integer
        nbytes = struct.unpack("i",
                               fcntl.ioctl(sock, OPT, "    "))[0]

    else:
        raise AttributeError("FIONREAD")

    return nbytes


# The return value is the number of un-ACKed packets.  This function only
# works on Linux.  All other OSes get a socket exception with errno EOPNOTSUPP.
def get_unacked_packet_count(sock):
    """
    Number of un-ACKed packets. UNIX ONLY: Other OSes see EOPNOTSUPP.
    Not intended for external use.

    Args:
        sock (Socket): Socket to check for un-ACKed packets

    Returns:
        int: Number of un-ACKed packets

    Raises:
        SocketException (EOPNOTSUPP): On non-Unix systems
    """
    OPT = getattr(socket, "TCP_INFO", None)
    if OPT is None:
        # Give an error since TCP_INFO is not supported on this system.
        s_errno = getattr(errno, 'EOPNOTSUPP',
                          getattr(errno, "ENOTSUP", errno.EIO))
        raise socket.error(s_errno, "getsockopt(TCP_INFO) is not supported")

    if os.uname()[0] != "Linux":
        # We only have the unpacking for the Linux version of getsockopt.
        s_errno = getattr(errno, 'EOPNOTSUPP',
                          getattr(errno, "ENOTSUP", errno.EIO))
        raise socket.error(s_errno, "getsockopt(TCP_INFO) format is not known")

    # Get the TCP socket information for this socket.
    raw_tcp_info = sock.getsockopt(socket.SOL_TCP, socket.TCP_INFO, 92)

    # We need to pull the information out of the string returned from
    # getsockopt().  See /usr/include/netinet/tcp.h for more information
    # about the other fields in this C struct.
    tcp_info = struct.unpack("BBBBBBBIIIIIIIIIIIIIIIIIIIII", raw_tcp_info)

    # Linux specific location for un-acked packets in the tuple.
    UNACKED = 12
    return tcp_info[UNACKED]


def __get_socket_state(fd):
    """
    Return socket state of supplied FD.
    Not intended for external use.

    Args:
        fd (Socket): File Descriptor to read state

    Returns:
        String or NONE: Socket state according to tcp socket table
    """
    if os.uname()[0] == "Linux":
        import stat
        try:
            # Determine the current socket state.

            # This table of values is from /usr/include/linux/tcp.h.
            tcp_states = {1: "ESTABLISHED",
                          2: "SYN_SENT",
                          3: "SYN_RECV",
                          4: "FIN_WAIT1",
                          5: "FIN_WAIT2",
                          6: "TIME_WAIT",
                          7: "CLOSE",
                          8: "CLOSE_WAIT",
                          9: "LAST_ACK",
                          10: "LISTEN",
                          11: "CLOSING"
                          }
            # First get the inode (as a string for comparison).
            inode = str(os.fstat(fd)[stat.ST_INO])[:-1]

            # Second, read the entire table of tcp sockets.
            net_tcp = open("/proc/net/tcp", "r")
            net_tcp_data = net_tcp.readlines()
            net_tcp.close()

            # Find the entry that corresponds to this socket.
            state = ""
            for line in net_tcp_data:
                result = line.find(inode)
                if result >= 0:
                    state = line[33:37]
                    break

            return tcp_states.get(int(state, 16), "UNKNOWN")
        except (socket.error, ValueError, IOError, OSError), msg:
            # We need to catch IOError or OSError incase the open of
            # /proc/net/tcp fails.  On 9-10-2007, an encp gave a traceback
            # opening /proc/net/tcp because of "No such file or directory".
            # How that can happen to a file in /proc, I don't know.
            Trace.log(e_errors.ERROR,
                      "__get_socket_state(): /proc/net/tcp: %s" % str(msg))

    return None


def log_socket_state(sock):
    """
    Dump as much of the socket state as can be determined to stderr.
    Not intended for external use.

    Args:
        sock (Socket): Socket to have its state dumped to stderr

    Returns:
        None
    """
    # According to python documentation when recv() returns the empty
    # string the other end has closed the connection.

    try:
        # Verify if there is an error on the socket.
        socket_error = sock.getsockopt(socket.SOL_SOCKET,
                                       socket.SO_ERROR)
        if socket_error != 0:
            Trace.log(e_errors.ERROR,
                      "socket state: pending SO_ERROR: %s"
                      % (socket_error,))
    except socket.error as msg:
        Trace.log(e_errors.ERROR,
                  "socket state: getsockopt(SO_ERROR): %s"
                  % str(msg))

    try:
        # Verify if the connection is still up.
        peer_name = sock.getpeername()
    except socket.error as msg:
        peer_name = None
        Trace.log(e_errors.ERROR,
                  "socket state: getpeername(): %s" % str(msg))

    Trace.log(e_errors.ERROR,
              "socket state: received no data from %s" % peer_name)

    # Log the current socket state (only works on Linux).
    socket_state = __get_socket_state(sock.fileno())
    Trace.log(e_errors.ERROR,
              "socket state: socket state: %s" % str(socket_state))

    # It would be useful to output the number of bytes in the
    # read buffer.  Python does not yet support it.
    try:
        nbytes = get_socket_read_queue_length(sock)

        Trace.log(e_errors.ERROR,
                  "socket state: fcntl(FIONREAD): %s"
                  % (str(nbytes),))
    except AttributeError:
        # FIONREAD not known on this system.
        pass
    except IOError as msg:
        Trace.log(e_errors.ERROR,
                  "socket state: ioctl(FIONREAD): %s" % (str(msg),))

    # Get any mac addresses.
    arpGetFunc = getattr(Interfaces, "arpGet", None)
    if peer_name and arpGetFunc:
        try:
            mac_addresses = arpGetFunc(peer_name[0])
            if mac_addresses:
                Trace.log(e_errors.ERROR,
                          "socket state: arp get[%s]: %s" %
                          (peer_name[0], str(mac_addresses)))

        except Exception as msg:
            Trace.log(e_errors.ERROR,
                      "socket state: arp get[%s]: %s" %
                      (peer_name[0], str(msg)))


###############################################################################
###############################################################################

# get an unused tcp port for control communication
def get_callback(ip=None):
    """
    Get an unused tcp port for control communication. Defaults to config
    server host. Called frequently in codebase.

    Args:
        ip (bytearray or str or unicode or None, default:None):
            callback destination IP address

    Returns:
        tuple (str, int, Socket): host, port, initialized TCP socket
    """
    config = host_config.get_config()
    if ip is None:
        if config:
            ip = config.get('hostip')
        if not ip:
            ip = host_config.get_default_interface_ip()
    address_family = socket.getaddrinfo(ip, None)[0][0]
    s = socket.socket(address_family, socket.SOCK_STREAM)
    s.bind((ip, 0))
    host, port = s.getsockname()[0:2]

    return host, port, s


def connect_to_callback(ip_addr, interface_ip=None, timeout=30):
    """
    Create a connected socket, blocking socket to target IP.
    Not intended for external use. Called once in generic_client.

    Args:
        ip_addr (bytearray or str or unicode): Socket destination ip
        interface_ip (bytearray or str or unicode, default:None): Particular
            source IP to use if source is multihomed
        timeout (int, default:30): Timeout for checking socket status, in secs

    Returns:
        Socket: Connected socket object, in blocking mode

    Raises:
        SocketError: Failure to initialize, connect, or get socket status
    """
    hostinfo = socket.getaddrinfo(ip_addr[0], None)
    try:
        # Create the socket.
        connect_socket = socket.socket(hostinfo[0][0], socket.SOCK_STREAM)
    except socket.error:
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

    # Put the socket into non-blocking mode.
    flags = fcntl.fcntl(connect_socket.fileno(), fcntl.F_GETFL)
    fcntl.fcntl(connect_socket.fileno(), fcntl.F_SETFL,
                flags | os.O_NONBLOCK)

    # Attempt to use one specific IP address on the local multihomed machine.
    try:
        if interface_ip:
            connect_socket.bind((interface_ip, 0))
    except socket.error:
        connect_socket.close()
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

    try:
        connect_socket.connect(ip_addr)
        # error = 0 #MWZ: pychecker questioned this line.
    except socket.error as msg:
        # We have seen that on IRIX, when the three-way handshake is in
        # progress, we get an EISCONN error.
        if hasattr(errno, 'EISCONN') and msg[0] == errno.EISCONN:
            pass
        # The TCP handshake is in progress.
        elif msg[0] == errno.EINPROGRESS:
            pass
        # A real or fatal error has occurred.  Handle accordingly.
        else:
            message = "Connecting to socket failed immediately."
            Trace.log(e_errors.ERROR, message)
            Trace.trace(12, message)
            connect_socket.close()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

    # Check if the socket is open for reading and/or writing.
    r, w = None, None
    while 1:
        try:
            r, w, unused = select.select([connect_socket],
                                         [connect_socket], [], timeout)
            break
        except (socket.error, select.error), msg:
            if errno.EINTR == msg.args[0]:
                # Screen out interruptions from signals.
                continue
            else:
                connect_socket.close()
                raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            try:
                connect_socket.close()
            except:
                pass
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

    if r or w:
        # Get the socket error condition...
        rtn = connect_socket.getsockopt(socket.SOL_SOCKET,
                                        socket.SO_ERROR)
        # error = 0 #MWZ: pychecker questioned this line.
    # If the select didn't return sockets ready for read or write, then the
    # connection timed out.
    else:
        connect_socket.close()
        raise socket.error(errno.ETIMEDOUT, os.strerror(errno.ETIMEDOUT))

    # If the return value is zero then success, otherwise it failed.
    if rtn != 0:
        message = "Connecting to socket failed later."
        Trace.log(e_errors.ERROR, message)
        Trace.trace(12, message)
        connect_socket.close()
        raise socket.error(rtn, os.strerror(rtn))

    # Restore flag values to blocking mode.
    fcntl.fcntl(connect_socket.fileno(), fcntl.F_SETFL, flags)

    return connect_socket


###############################################################################
###############################################################################

# send with a timeout
# Normally, this should return the number of bytes written/sent.  However,
# it will return the empty string for error conditions.
def timeout_send(sock, msg, timeout=15 * 60):
    """
    Send message on socket with a timeout (default 15 min).
    Not intended for external use.

    Args:
        sock (Socket): Socket on which to send the message
        msg (str): Message to send
        timeout (int, default:900): Send timeout in seconds.

    Returns:
        Tuple (int, str): Bytes written, error message

    Raises:
        SocketError: Unable to retrieve writable fds from socket
    """
    total_start_time = time.time()
    timeout_time = total_start_time + timeout

    error_string = ""

    # Loop until the timeout has passed, a hard error occurs (AKA where an
    # exception is raised) or the message has really arrived.
    while timeout_time > time.time():
        time_left = max(timeout_time - time.time(), 0.0)
        try:
            _, fds, _ = select.select([], [sock], [], time_left)
        except (select.error, socket.error) as msg:
            if msg.args[0] in [errno.EINTR, errno.EAGAIN]:
                continue

            # Re-raise the exception for write_raw() to handle.
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

        if sock in fds:
            # We got our socket.
            break

        # Current theory: On Linux the pipe buffer needs to be completely
        # empty for select to return that the pipe is writable.
        # So lets keep looping for a while.

    if isinstance(sock, types.IntType):  # In case sock is an fd for a pipe.
        nwritten = os.write(sock, msg)
    else:
        nwritten = sock.send(msg)

    return nwritten, error_string


# this function is to optimize code in write raw
def _send_raw(sock, message, message_length, timeout):
    """
    Send data on a socket, continuing where left off if not all data is sent
    successfully. Retry until all data is sent or 0 bytes are written by send
    request. Return failure if too much or too little data is sent
    successfully, return success otherwise.
    Not intended for external use.

    Args:
        sock (Socket): Socket on which to send data
        message (Object): Data to send
        message_length (int): Bytes of data that should be sent
        timeout (int): Timeout for each send request

    Returns:
        Tuple (int, str): Return code (0:success, 1:failure), error string
    """
    ptr = 0
    error_string = ""
    while ptr < message_length:
        nwritten, error_string = timeout_send(sock, message[ptr:],
                                              timeout)
        if not isinstance(nwritten, types.IntType) or nwritten <= 0:
            break
        ptr = ptr + nwritten
    if ptr < message_length:
        error_string = "short write: expected %s, sent %s" \
                       % (message_length, ptr)
        return 1, error_string
    if ptr != message_length:
        error_string = "bad write: message length (expected %s, sent %s) %s" \
                       % (message_length, ptr, error_string)
        return 1, error_string
    return 0, ""


# Send a message, with bytecount and rudimentary security
## Note: Make sure to consider that sock could be a socket object, socket
##       fd or pipe fd.
def write_raw(sock, msg, timeout=15 * 60):
    """
    Send a message to a socket, along with message length and hash. in detail:
        1. Uses _send_raw to send protocol prefix (PROTO001 for large msg or "")
        2. Uses _send_raw to send length of message
        3. Uses _send_raw to send "signature" of message (ENSTOR + a salt)
        4. Re-implements _send_raw to send msg
        5. Uses _send_raw to send Adler32 checksum of message
    Not intended for external use.

    Args:
        sock (Socket or int): Socket, FD or pipe FD on which to send message
        msg (str): Message to send
        timeout(int, default:900): timeout for each send request in seconds

    Returns:
        Tuple (int, str):
            (0, "") if success
            (1, error_msg) otherwise
    """
    max_pkt_size = 16384
    proto = ""
    try:
        # Determine the length of the payload.
        msg_len = len(msg)
        if msg_len > 99999999:  # the biggest 8-digit number allowed by old implementation
            # new protocol
            ml_format = "%%0%sd" % MSG_LEN_POSITIONS
            proto = PROTOCOL
        else:
            # old protocol
            ml_format = "%%0%sd" % MSG_LEN_POSITIONS_OLD

        # First part of the message sent is MSG_LEN_POSITIONS characters consisting of the
        # length of the payload part of the message.  Put these bytes together
        # and determine the length.
        msg_msg_len = ml_format % (msg_len,)
        msg_len_len = len(str(msg_msg_len))

        # Second part of the message sent is 8 characters of Enstore
        # specific bytes.  Put these bytes together and determine the length.
        salt = random.randint(11, 99)
        msg_signature = "ENSTOR%s" % (salt,)
        msg_signature_len = len(msg_signature)

        # Fourth part of the message sent is the 32bit adler32 CRC of the
        # payload of the message.  Put these bytes together and determine
        # the length.
        checksum_msg = hex8(checksum.adler32(salt, msg, msg_len))
        checksum_len = len(checksum_msg)

        # send the protocol prefix if there is one
        if proto:
            e, err_msg = _send_raw(sock, proto, len(proto), timeout)
            if e:
                return e, err_msg

        # Now actually write out the length of the payload to the socket.
        e, err_msg = _send_raw(sock, msg_msg_len, msg_len_len, timeout)
        if e:
            return e, err_msg

        # This time write out the 'signature'.
        e, err_msg = _send_raw(sock, msg_signature, msg_signature_len, timeout)
        if e:
            return e, err_msg

        # Write the payload to the socket.
        ptr = 0
        while ptr < msg_len:
            nwritten, error_string = timeout_send(sock,
                                                  msg[ptr:ptr + max_pkt_size], timeout)
            if not isinstance(nwritten, types.IntType) or nwritten <= 0:
                break
            ptr = ptr + nwritten
        if ptr < msg_len:
            error_string = "short write: message expected %d, sent %s" \
                           % (msg_len, ptr)
            return 1, error_string
        if ptr != msg_len:
            error_string = "bad write: message expected %d, sent %s" \
                           % (msg_len, ptr)
            return 1, error_string

        # Lastly, write out the checksum of the payload.
        e, err_msg = _send_raw(sock, checksum_msg, checksum_len, timeout)
        if e:
            return e, err_msg

        return 0, ""
    except (socket.error, select.error, OSError), detail:
        error_string = "write_raw: socket.error %s" % (detail,)
        # Trace.log(e_errors.ERROR, error_string)
        return 1, error_string
        # XXX Further sends will fail, our peer will notice incomplete message


write_tcp_raw = write_raw


# send a message over the network which is a Python object
def write_tcp_obj(sock, obj, timeout=15 * 60):
    """
    Serialize an object via repr() and send over a socket.
    Deprecated in favor of write_tcp_obj_new, which uses cPickle.dumps(). Still
    called from several places in the codebase.

    Args:
        sock (Socket or int): Socket or FD to send object over
        obj (Object): repr()-compatible object to send over socket
        timeout (int, default:900): timeout for each send request in seconds

    Returns:
        int: Response code (0 for success, 1 for failure)

    Raises:
        EnstoreError: If there is an error returned in send function
    """
    if not isinstance(sock, types.IntType) and not hasattr(sock, "fileno"):
        raise TypeError("expected integer socket file descriptor or "
                        "socket object; received %s instead" % (str(sock),))
    ### When we want to go strictly to cPickle use the following line.
    #    return write_tcp_obj_new(sock, obj, timeout)

    rtn, e = write_tcp_raw(sock, repr(obj), timeout)

    if e:
        if not isinstance(sock, types.IntType):
            log_socket_state(sock)  # Log the state of the socket.
        Trace.log(e_errors.ERROR, e)
        raise e_errors.EnstoreError(None, e, e_errors.NET_ERROR)

    return rtn


# send a message over the network which is a Python object
def write_tcp_obj_new(sock, obj, timeout=15 * 60):
    """
    Serialize an object via cPickle.dumps() and send over a socket.
    Called from a few places in the codebase.

    Args:
        sock (Socket or int): Socket or FD to send object over
        obj (Object): cPickle.dumps()-compatible object to send over socket
        timeout (int, default:900): timeout for each send request in seconds

    Returns:
        int: Response code (0 for success, 1 for failure)

    Raises:
        EnstoreError: If there is an error returned in send function
    """
    if not isinstance(sock, types.IntType) and not hasattr(sock, "fileno"):
        raise TypeError("expected integer socket file descriptor or "
                        "socket object; received %s instead" % str(sock))

    rtn, e = write_tcp_raw(sock, cPickle.dumps(obj), timeout)

    if e:
        if not isinstance(sock, types.IntType):
            log_socket_state(sock)  # Log the state of the socket.
        Trace.log(e_errors.ERROR, e)
        raise e_errors.EnstoreError(None, e, e_errors.NET_ERROR)

    return rtn


# send a message to a co-process which is a Python object
def write_obj(fd, obj, timeout=15 * 60, verbose=True):
    """
    Serialize an object via cPickle.dumps() and send over an fd.
    Called only from migrate binaries.

    Args:
        fd (int): FD to send object over
        obj (Object): cPickle.dumps()-compatible object to send over socket
        timeout (int, default:900): timeout for each send request in seconds
        verbose (bool, default:True):
            True: Log and raise errors
            False: just return status

    Returns:
        int: Response code (0 for success, 1 for failure)

    Raises:
        EnstoreError: If verbose and there is an error in send function
    """
    rtn, e = write_raw(fd, cPickle.dumps(obj), timeout)

    if e and verbose:
        Trace.log(e_errors.ERROR, e)
        # raise e_errors.TCP_EXCEPTION #What should this be?
        raise e_errors.EnstoreError(None, e, e_errors.IOERROR)

    return rtn


###############################################################################
###############################################################################

# recv with a timeout
def timeout_recv(sock, nbytes, timeout=15 * 60):
    """
    Receive bytes from a socket or FD.
    Not intended for external use.

    Args:
        sock (Socket or int): Socket or FD on which to receive bytes
        nbytes (int): Number of bytes to read
        timeout (int, default:900): Total timeout for receive request, in seconds

    Returns:
        Tuple (str, str): Data or "", error
    """
    total_start_time = time.time()
    timeout_time = total_start_time + timeout

    error_string = ""
    data_string = ""

    # Loop until a timeout has passed, a hard error occurs or
    # the message has really arrived.
    while timeout_time > time.time():
        try:
            time_left = max(timeout_time - time.time(), 0.0)
            fds, _, _ = select.select([sock], [], [], time_left)
        except (select.error, socket.error) as msg:
            if msg.args[0] in [errno.EINTR, errno.EAGAIN]:
                continue
            error_string = "timeout_recv(): %s" % str(msg)
            # Return to handle the error.
            return "", error_string
        if sock not in fds:
            # Hopefully, this situation is different from other situations
            # that were all previously lumped together as "error".
            continue

        read_len = nbytes - len(data_string)
        try:
            if isinstance(sock, types.IntType):
                data_string = data_string + os.read(sock, read_len)
            else:
                data_string = data_string + sock.recv(read_len)
        except socket.error, msg:
            error_string = "timeout_recv(): %s" % str(msg)
            # Return to handle the error.
            return "", error_string
        if data_string == "":
            # According to python documentation when recv() returns the empty
            # string the other end has closed the connection.

            # Log the time spent waiting.
            error_string = "timeout_recv(): time passed: %s sec of %s sec" % \
                           (time.time() - total_start_time, timeout)
            # Trace.log(e_errors.ERROR, error_string)
        if len(data_string) != nbytes:
            # Keep trying until we get everything we want.
            continue

        return data_string, error_string

    # timedout
    error_string = "timeout_recv(): timedout"
    return "", error_string


# read_raw - return tuple of message read and error string.  One or the other
# should be returned as an empty string.
def read_raw(fd, timeout=15 * 60):
    """
    Read message off an FD, determining byte length based on first byte received.
    Not intended for external use.

    Args:
        fd (int): File Descriptor to receive from
        timeout (int, default:900): Timeout for each recv request, in seconds

    Returns:
        Tuple (str, str): (data, "") or ("", error)
    """
    # Read in the length of the payload part of the message or the protocol string for a new protocol
    tmp, error_string = timeout_recv(fd, 8, timeout)  # the message length
    len_tmp = len(tmp)
    if len_tmp != 8:
        error_string = "%s; read_raw: wrong bytecount (%d) '%s'" % \
                       (error_string, len_tmp, tmp)
        return "", error_string
    try:
        bytecount = int(tmp)
    except (ValueError, TypeError):
        if tmp[:8] == PROTOCOL:
            # new protocol
            # the bytecount follows
            # Read in the length of the payload part of the message
            tmp, error_string = timeout_recv(fd, MSG_LEN_POSITIONS, timeout)  # the message length
            len_tmp = len(tmp)
            if len_tmp != MSG_LEN_POSITIONS:
                error_string = "%s; read_raw: wrong bytecount (%d) '%s'" % \
                               (error_string, len_tmp, tmp)
                return "", error_string
            try:
                bytecount = int(tmp)
            except (ValueError, TypeError):
                error_string = "%s; read_raw: bad bytecount '%s'" % \
                               (error_string, tmp,)
                return "", error_string
        else:
            error_string = "%s; read_raw: bad bytecount '%s'" % \
                           (error_string, tmp,)
            return "", error_string

    # Read in the signature.
    tmp, error_string = timeout_recv(fd, 8, timeout)  # the 'signature'
    if len(tmp) != 8 or tmp[:6] != "ENSTOR":
        error_string = "%s; read_raw: invalid signature '%s'" % \
                       (error_string, tmp,)
        return "", error_string

    # Extract the salt from the signature.
    salt = int(tmp[6:])

    # Read in the payload and verify it is consistent with what we expected.
    msg = ""
    while len(msg) < bytecount:
        tmp, error_string = timeout_recv(fd, bytecount - len(msg), timeout)
        if not tmp:
            break
        msg = msg + tmp
    if len(msg) != bytecount:
        error_string = "%s; read_raw: bytecount mismatch %s != %s" \
                       % (error_string, len(msg), bytecount)
        return "", error_string

    # Read in the adler32 CRC and verify it is consistent with what we
    # expected.
    tmp, error_string = timeout_recv(fd, 8, timeout)
    crc = long(tmp, 16)  # XXX
    mycrc = checksum.adler32(salt, msg, len(msg))
    if crc != mycrc:
        error_string = "%s; read_raw: checksum mismatch %s != %s" \
                       % (error_string, mycrc, crc)
        return "", error_string
    return msg, ""


read_tcp_raw = read_raw


# receive a message over the network which is a Python object
def read_tcp_obj(sock, timeout=15 * 60):
    """
    Receive a serialized python object from a socket or FD. Attempts to
    deserialize object via cPickle.loads().
    Called sparsely throughout codebase.

    Args:
        sock (Socket or int): Socket or FD to receive from
        timeout (int, default:900): Timeout for each receive request, in seconds

    Returns:
        Object or None: Received object
    """
    if not isinstance(sock, types.IntType) and not hasattr(sock, "fileno"):
        raise TypeError("expected integer socket file descriptor or "
                        "socket object; received %s instead" % str(sock))

    s, e = read_tcp_raw(sock, timeout)
    if not s:
        if not isinstance(sock, types.IntType):
            log_socket_state(sock)  # Log the state of the socket.

        # Gather additional information and log the error.
        try:
            peername = sock.getpeername()
        except (socket.error, socket.herror, socket.gaierror):
            peername = "unknown"
        error_string = "%s from %s" % (e, peername)
        Trace.log(e_errors.ERROR, error_string)

        raise e_errors.EnstoreError(None, e, e_errors.NET_ERROR)

    try:
        obj = cPickle.loads(s)
    except (cPickle.PickleError, cPickle.PicklingError,
            cPickle.UnpickleableError, cPickle.UnpicklingError):
        try:
            obj = en_eval(s)
        except SyntaxError:
            obj = None

    return obj


# receive a message over the network which is a Python object
def read_tcp_obj_new(sock, timeout=15 * 60, exit_on_no_socket=False):
    """
    Receive a serialized python object from a socket or FD. Attempts to
    deserialize object via cPickle.loads().
    Called sparsely throughout codebase.

    Args:
        sock (Socket or int): Socket or FD to receive from
        timeout (int, default:900): Timeout for each receive request in seconds
        exit_on_no_socket (bool, default:False): Quit if no data is returned

    Returns:
        Object or None: Received object
    """
    if not isinstance(sock, types.IntType) and not hasattr(sock, "fileno"):
        raise TypeError("expected integer socket file descriptor or "
                        "socket object; received %s instead" % (str(sock),))

    s, e = read_tcp_raw(sock, timeout)
    if not s:
        if exit_on_no_socket:
            return
        if not isinstance(sock, types.IntType):
            log_socket_state(sock)  # Log the state of the socket.

        # Gather additional information and log the error.
        try:
            peername = sock.getpeername()
        except (socket.error, socket.herror, socket.gaierror):
            peername = "unknown"
        error_string = "%s from %s" % (e, peername)
        Trace.log(e_errors.ERROR, error_string)

        # raise e_errors.TCP_EXCEPTION
        raise e_errors.EnstoreError(None, e, e_errors.NET_ERROR)

    return cPickle.loads(s)


# receive a message from a co-process which is a Python object
def read_obj(fd, timeout=15 * 60, verbose=True):
    """
    Receive object from an FD.
    Called only from migrate scripts.

    Args:
        fd (int): FD to recv object on
        timeout (int, default:900): timeout for each recv request in seconds
        verbose (bool, default:True):
            True: Log and raise errors
            False: just return status

    Returns:
        Object: Received object deserialized with cPickle.loads()

    Raises:
        EnstoreError: If verbose and there is an error in send function
    """
    s, e = read_raw(fd, timeout)
    if not s:
        if verbose:
            Trace.log(e_errors.ERROR, e)

        # raise e_errors.TCP_EXCEPTION #What should this be?
        raise e_errors.EnstoreError(None, e, e_errors.IOERROR)

    return cPickle.loads(s)


if __name__ == "__main__":  # pragma: no cover
    Trace.init("CALLBACK")
    Trace.trace(6, "callback called with args " + repr(sys.argv))

    c = get_callback()
    Trace.log(e_errors.INFO, "callback exit ok callback=" + repr(c))
