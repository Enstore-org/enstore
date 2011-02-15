#!/usr/bin/env python
#
# derived from apache qpid api examples "server" 
#
# version to work with qpid 0.8 : message constructor does not have "to" field anymore 

import copy
import optparse, sys, traceback
import time

#import qpid.messaging
from qpid.messaging import Connection, Message, ReceiverError, SendError
from qpid.util import URL
from subprocess import Popen, STDOUT, PIPE
from qpid.log import enable, DEBUG, WARN

# send ack every n_ack messages
n_ack = 100

parser = optparse.OptionParser(usage="usage: %prog [options] ADDRESS ...",
                               description="handle requests from the supplied address.")
parser.add_option("-b", "--broker", default="localhost",
                  help="connect to specified BROKER (default %default)")
parser.add_option("-i", "--reconnect-interval", type="float", default=3,
                  help="interval between reconnect attempts")
parser.add_option("-r", "--reconnect", action="store_true",
                  help="enable auto reconnect")
parser.add_option("-l", "--reconnect-limit", type="int",
                  help="maximum number of reconnect attempts")
parser.add_option("-v", dest="verbose", action="store_true",
                  help="enable logging")
parser.add_option("-d", dest="debug", action="store_true",
                  help="enable debug printout")

opts, args = parser.parse_args()
debug = opts.debug

if opts.verbose:
  enable("qpid", DEBUG)
else:
  enable("qpid", WARN)

#url = URL(opts.broker)
if args:
  addr = args.pop(0)
else:
  parser.error("address is required")

conn = Connection(opts.broker,
                  reconnect=opts.reconnect,
                  reconnect_interval=opts.reconnect_interval,
                  reconnect_limit=opts.reconnect_limit)

def set_reply(msg):
    """
    construct reply message
    with the same content, content_type, correlation_id of the original message.
    The 'reply_to' field it tested 
    """
    
    result = Message(msg.content, correlation_id=msg.correlation_id )               
    return result

try:
  conn.open()
  ssn = conn.session()
  rcv = ssn.receiver(addr)
  # TODO : reply address is fixed to test performance issues
  # this is a hack
  snd = ssn.sender("udp2amq_131.225.13.187_7700")
  print "echo server  - echoing messages sent to %s" % (addr)

  m_count = 0

  while True:
    msg = rcv.fetch()
    m_count = m_count +1

    #-> timing
    if debug:
        print "----------------------------------------"
        print "debug echo server  - received message %s" % (msg)
        t0 = None
        try:
            c = msg.content
            t0 = c["t"]
            t = time.time()
            print "got      dt=%s" % ((t-t0)*1000)
        except:
            pass
    #<- timing
    
    ## try it here for now :
    #ssn.acknowledge(msg)
    ## -> timing
    #if debug and t0:
    #    print "ack      dt=%s" % ((time.time()-t0)*1000)
    ##<- timing

    if not msg.reply_to:
        continue
    
    response = set_reply(msg)
    if debug:
        print "debug echo server  - response %s" % (response)

    if not response:
        #print "echo : no response, acknowledge"
        #   ssn.acknowledge(msg)
        continue

    # TODO : see below 
    # snd = None
    try:
      # TODO: fixme: dynamic sender creation is replaced by fixed sender for tests
      # snd = ssn.sender(response.to)
      snd.send(response, sync=False)
    except SendError, e:
      print e

    # ack original message so it will be discharged from queue and not will not be repeated 
    #print "echo : reply sent, acknowledge input"
    #if n_ack == 0  or m_count % n_ack == 0 :
    #    ssn.acknowledge(msg)
    
    # -> timing
    if debug and t0:
        print "sent     dt=%s" % ((time.time()-t0)*1000)
    #<- timing

    #if snd is not None:
    #  snd.close()
    #  del snd

    # ack original message so it will be discharged from queue and not will not be repeated 
    #print "echo : reply sent, acknowledge input"

    if n_ack == 0  or m_count % n_ack == 0 :
        ssn.acknowledge(sync=False)    
        if debug and t0:
            print "ssn.ack  dt=%s" % ((time.time()-t0)*1000)

    del msg
    del response

except ReceiverError, e:
    print e
except KeyboardInterrupt,e :
    if ssn:
        print "Ack all unacknowledged messages in session"
        ssn.acknowledge(sync=False)
    pass

conn.close()
