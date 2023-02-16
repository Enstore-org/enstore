#!/usr/bin/env python

##############################################################################
#
# $Id$
#
##############################################################################


'''
    udp2amq - enstore udp to amqp proxy
'''

# system imports
import sys
import time
import types
from multiprocessing import Process
import logging

# qpid / amqp
from qpid.messaging import Message 
from qpid.messaging import ReceiverError, SendError
import Queue

# enstore imports
from udp_server import UDPServer

# enstore cache imports
# refactoring: use frosen version of the file before it tested to work with next version
#from cache.messaging.client_1_1_2_3 import EnQpidClient
from cache.messaging.client import EnQpidClient

def normalize_ticket(obj):
    """Normalize content of enstore ticket received from qpid by convering unicode to ascii and lists to tuples
    """
    if type(obj) in [types.NoneType, str, int, long, bool, float]:
        return obj
    elif type(obj) == unicode:
        return obj.encode()
    elif type(obj) is dict:
        d = {}
        for k,v in obj.iteritems():
            # self.trace.debug(" k,v %s,%s",k,v )
            d[normalize_ticket(k)] = normalize_ticket(v)
        return d
    elif type(obj) in [list, tuple]:
        return tuple(map(normalize_ticket,obj))

    return obj

class UDP2amq(UDPServer):

    def __init__(self, udp_srv_addr, receive_timeout=60., use_raw=None, amq_broker=("localhost",5672), target_addr=None, auto_ack=True ):
        '''
        Constructor
        Creates UDP server with udp_srv_addr = (host, port) and optional parameters 'receive_timeout' and 'use_raw'
        amq_broker=(host,port) is address to be used in serve_udp() to connect to amqp broker
        target_addr is amqp/qpid address to exchange messages with in the format described at  
            qpid.messaging.endpoints Session
        The qpid "address" specifies destination node and  options. If  it is not specified, the messages will be sent 
        to amqp queue  with target address  udp2amq_UDPHOST_UDPPORT, where "UDPHOST" and UDPPORT are 
        replaced by values of host, port of the UDP server.
        '''
        self.shutdown = False
        self.finished = False
        
        self.log = logging.getLogger('log.encache.udp2amq' )
        self.trace = logging.getLogger('trace.encache.udp2amq' )
        
        UDPServer.__init__(self, udp_srv_addr, receive_timeout, use_raw)

        # # AMQP to UDP, receive (fetch) from qpid queue
        (host,port) = self.get_server_address()
        self.trace.debug("Host: %s %s", host, port )
        self.myaddr = self._hp2q(host, port)
        myaddr = "%s; {create: always}"%(self.myaddr,)
        self.qpid_client_r = EnQpidClient(amq_broker, myaddr)

        # UDP to AMQP, send to qpid queue
        self.trace.debug("Address: %s", target_addr )
        self.target_addr = target_addr        
        t_a = "%s; {create: always}"%(target_addr,)
        self.qpid_client_s = EnQpidClient(amq_broker, target=t_a)
        
        self.auto_ack = auto_ack 

    def _hp2q(self,host,port):
        " get name of the reply queue "
        return "udp2amq_%s_%s" %(host, port)

    def serve_udp(self):
        """
        Read UDP message and send it to amq queue
        """
        self.qpid_client_s.start()
        try:
            while not self.shutdown:
                try:
                    ticket = self.do_request() # udpsrv
                except KeyboardInterrupt:
                    self.log.info("Keyboard interrupt at serve_udp thread")
                    break
    
                if not ticket:
                    continue
    
                self.trace.debug("serve_udp()  - got ticket=%s", ticket)
    
                # forward ticket to AMQP
                # @todo : set reply_to depending on 'r_a'
                if type(ticket) == types.DictType and (ticket.get("r_a", None) or ticket.get("ra", None) ):
                    reply_to = self.myaddr
                else:
                    reply_to = None
    
                self.trace.debug("serve_udp(): to=%s reply_to=%s", self.target_addr, reply_to )
    
                msg = Message(content=ticket, reply_to=reply_to)
                                
                try:
                    self.qpid_client_s.send(msg, sync=False)
                    self.trace.debug("serve_udp() : message sent, msg=%s", msg )
                except SendError, e:
                    self.log.error("serve_udp() : sending message, error=%s", e )
                    continue
    
                # Send reply to UDP caller if send to AMQ was successful but do not retry if it failed.
                # self.reply_to_caller(ticket) # I'm udpsrv
        finally:
            self.qpid_client_s.stop()

    def _fetch_qpid_reply(self):
        try:
            msg = self.qpid_client_r.fetch()

            # even there is no reply_to in message (such as in reply message) we want to ack it
            # make sure msg is not None, otherwise it will ack all messages in session
            if self.auto_ack and msg:
                try:
                    # @todo (?) : acknowledge message after it has been replied by udp, when it with "wait"
                    # @todo : close() snd.
                    self.trace.debug("_fetch_qpid_reply(): sending acknowledge")
                    self.qpid_client_r.ssn.acknowledge(msg)             
                except:
                    exc, emsg = sys.exc_info()[:2]
                    self.log.error("Can not send auto acknowledge for the message. Exception e=%s msg=%s", 
                                   str(exc), str(emsg) )    
                    pass
            return msg
        except Queue.Empty:
            return None
        except ReceiverError, e:
            self.log.error("_fetch_qpid_reply(): fetching message, error=%s", e )
            return None
        
    def serve_qpid(self):
        """
        read qpid messages from queue
        @todo propagate error in udp send to qpid ?  
        """
        self.qpid_client_r.start() 
        try:
            while not self.shutdown:
                reply =  self._fetch_qpid_reply()
                if not reply:
                    continue
                
                self.trace.debug("serve_qpid()  - got qpid reply=%s",reply)
     
                # relay ticket to UDP
                # convert lists to tuples to make getsockaddrarg() happy, replace unicode by ascii str
                ticket = normalize_ticket(reply.content)

                try:
                    self.trace.debug("serve_qpid()  - received reply, ticket %s",ticket)
                 
                    if type(ticket) == types.DictType and ticket.get("r_a", None):          
                        self.reply_to_caller(ticket)
                    elif type(ticket) == types.DictType and ticket.get("ra", None):
                        self.reply_with_address(ticket)
                    else:
                        self.log.warning("serve_qpid()  - no reply address in reply, ticket %s.",ticket)
                        continue
                except:
                    exc, emsg = sys.exc_info()[:2]
                    self.log.error("serve_qpid()  - forwarding reply to udp client, (exception, msg)= %s %s", str(exc), str(emsg) )
        finally:
            self.qpid_client_r.stop()

    def start(self):
        # start serving UDP in separate process
        self.qpid_proc = Process(target=self.serve_qpid) 
        self.udp_proc = Process(target=self.serve_udp) 

        # first start "reply" processing, then direct feed
        self.qpid_proc.start()                
        self.udp_proc.start()

    def stop(self):
        # tell serving thread to stop and wait until it finish    
        self.shutdown = True
        
        # stop direct feed first
        self.udp_proc.join()
        # @todo: do we need to set extra delay to allow replies to be processed by remote server?
        self.qpid_proc.join()
        
if __name__ == "__main__":   # pragma: no cover    
    import cache.en_logging.config_test_unit
    
    #cache.en_logging.config_test_unit.set_logging_console()
    cache.en_logging.config_test_unit.set_logging_enstore(name="U2A_UNIT_TEST")
    
    # instantiate UDP2amq server
    u2a_srv = UDP2amq(('dmsen06.fnal.gov', 7700), use_raw=1, amq_broker=("dmsen04.fnal.gov",5672), target_addr="udp_relay_test")
    
    if u2a_srv.use_raw:
        u2a_srv.set_out_file()
        # start receiver thread or process
        u2a_srv.raw_requests.receiver()
       
    # start UDP2amq server
    u2a_srv.start()
    
    # stop UDP2amq server if there was keyboard interrupt
    while not u2a_srv.finished :
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            print "Keyboard interrupt at main thread"
            u2a_srv.stop()
            break
    
    del u2a_srv
    print "finished"
    