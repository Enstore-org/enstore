#!/usr/bin/env python

##############################################################################
#
# $Id$
#
##############################################################################


'''
    udp2amq - enstore udp to amqp proxy

    version based on Process
'''

# system imports
import sys
import time
import types
from multiprocessing import Process

# qpid / amqp
from qpid.messaging import Message 
from qpid.messaging import ReceiverError, SendError
import Queue

# enstore imports
from udp_server import UDPServer

# enstore cache imports
from cache.messaging.client import EnQpidClient

debug = False
timing = False

class UDP2amq(UDPServer):
    '''
    classdocs
    '''

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
        
        UDPServer.__init__(self, udp_srv_addr, receive_timeout, use_raw)
       
        (host,port) = self.get_server_address()
        if debug: 
            print "DEBUG UDP2amq __init__() Host: %s %s" % (host, port)
            print "DEBUG UDP2amq __init__() Address: %s" % target_addr
              
        self.myaddr = self._hp2q(host, port)
        
        # @todo shall not be here, sender shall know about target. For now set "to=" in message manually
        self.target_addr = target_addr
        self.qpid_client = EnQpidClient(amq_broker, self.myaddr, target=target_addr)

        self.auto_ack = auto_ack 

    def _hp2q(self,host,port):
        " get name of the reply queue "
        return "udp2amq_%s_%s" %(host, port)

    def serve_udp(self):
        """
        read UDP message and send it to amq queue, send reply to UDP caller if send to AMQ was successful (but no retries).

        """
        self.qpid_client.start()
        
        while not self.shutdown:
            try:
                ticket = self.do_request() # udpsrv
            except KeyboardInterrupt:
                print "Keyboard interrupt at serve_udp thread"
                break

            if not ticket:
                continue

            if debug:
                print "___________________________________________"
                print "DEBUG UDP2amq serve_udp()  - got ticket=%s" %(ticket,)

            if timing:
                t0 = None
                try:
                    t0 = ticket["t"]
                    t = time.time()
                    print "udp2amq: in   dt=%s" % ((t-t0)*1000)
                except:
                    pass
            

            # forward ticket to AMQP
            # @todo : set reply_to depending on 'r_a'

            if type(ticket) == types.DictType and (ticket.get("r_a", None) or ticket.get("ra", None) ):
                reply_to = self.myaddr
            else:
                reply_to = None

            if debug: 
                print "DEBUG UDP2amq serve_udp(): to=%s reply_to=%s" % (self.target_addr, reply_to)
                
            # qpid v0.6: msg = Message(content=ticket, to=self.target_addr, reply_to=reply_to)
            msg = Message(content=ticket, reply_to=reply_to)

            if timing:
                print "udp2amq: crea dt=%s" % ((time.time()-t0)*1000)
                            
            try:    
                # self.qpid_client.send(msg)
                self.qpid_client.send(msg, sync=False)
                if debug: print "DEBUG UDP2amq serve_udp() : message sent, msg=%s" % (msg)
            except SendError, e:
                if debug: print "DEBUG UDP2amq serve_udp() : sending message, error=", e
                continue

            if timing:
                print "udp2amq: sent dt=%s" % ((time.time()-t0)*1000)

            # reply to caller only if send to AMQ was successful. No retries.
            # TODO: verify with Sasha that we do not reply yet here (reply when qpid replies) 
            # self.reply_to_caller(ticket) # I'm udpsrv

    def _fetch_qpid_reply(self):
        try:
            msg = self.qpid_client.rcv.fetch()
                
            if timing:
                t0 = None
                try:
                    ticket = msg.content
                    t0 = ticket["t"]
                except:
                    pass
            
            if timing:
                print "udp2amq: repl. fetched dt=%s" % ((time.time()-t0)*1000)

            # even there is no reply_to in message (such as in reply message) we want to ack it
            # TODO: think do we need more control over reply ack
            #-x if self.auto_ack and msg.reply_to:
            # make sure msg is not None, otherwise it will ack all messages in session
            if self.auto_ack and msg:
                try:
                    # @todo : acknowledge message after it has been replied by udp, when it with "wait"
                    # @todo : close() snd.
                    if debug: print "DEBUG UDP2amq _fetch_qpid_reply(): sending acknowledge"
                    self.qpid_client.ssn.acknowledge(msg)
                    
                    if timing:
                        print "udp2amq: repl acked dt=%s" % ((time.time()-t0)*1000)                
                except:
                    exc, emsg = sys.exc_info()[:2]
                    if debug: print "DEBUG UDP2amq: Can not send auto acknowledge for the message. Exception e=%s msg=%s" % (str(exc), str(emsg))    
                    pass
            return msg
        except Queue.Empty:
            return None
        except ReceiverError, e:
            print e
            return None
        
    def serve_qpid(self):
        """
        read qpid messages from queue
        @todo propagate error in udp send to qpid ?  

        """
        self.qpid_client.start() 
        try:
            while not self.shutdown:
                reply =  self._fetch_qpid_reply()
                if not reply:
                    continue
                
                if debug: print "DEBUG UDP2amq serve_qpid()  - got qpid reply=%s" %(reply,)
     
                # relay ticket to UDP
                ticket = reply.content
                
                if timing:
                    t0 = None
                    try:
                        t0 = ticket["t"]
                        t = time.time()
                        print "udp2amq: got reply dt=%s" % ((t-t0)*1000)
                    except:
                        pass
                
                # ... use "reply" instead
                # back = self.send(msg=ticket, address, rcv_timeout = 10, max_send=3)

                try:
                    if debug: print "DEBUG UDP2amq serve_qpid()  - received reply, ticket %s." % (ticket)

                    if type(ticket) == types.DictType and ticket.get("r_a", None):
                        # workaround the issue:
                        #   amqp transfers tuple as a list
                        #   error "AF_INET address must be tuple, not list " in PyTuple_Check, in getsockaddrarg()
                        ticket['r_a'][0]=tuple(ticket['r_a'][0])
                        # this also is converted to list, but there is no complaint so far - uncomment if needed
                        #- ticket["r_a"] = tuple(ticket['r_a'])               
                        self.reply_to_caller(ticket)
                    elif type(ticket) == types.DictType and ticket.get("ra", None):
                        # ditto : convert reply address to tuple, see above
                        ticket['r_a'][0]=tuple(ticket['r_a'][0])
                        self.reply_with_address(ticket)
                    else:
                        if debug: print "DEBUG UDP2amq serve_qpid()  - no reply address in reply, ticket %s." % (ticket)
                        continue

                    if timing:
                        print "udp2amq: replied to caller dt=%s" % ((time.time()-t0)*1000)

                except:
                    exc, emsg = sys.exc_info()[:2]
                    print "DEBUG UDP2amq serve_qpid()  - forwarding reply to udp client, (exception, msg)= %s %s" % (str(exc), str(emsg))
        finally:
            self.qpid_client.stop()

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
        self.qpid_client.stop() # @todo
        # todo: set extra delay to allow replies to be processed by remote server?
        self.qpid_proc.join()
        
if __name__ == "__main__":    
    # instantiate UDP2amq server
    u2a_srv = UDP2amq(('dmsen06.fnal.gov', 7700), use_raw=1, amq_broker=("dmsen06.fnal.gov",5672), target_addr="udp_relay_test")
    
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
