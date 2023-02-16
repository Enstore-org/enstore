#!/usr/bin/env python

##############################################################################
#
# $Id$
##############################################################################


'''
    LMD - python prototype for Enstore File Cache Library Manager Dispatcher core functionality implementation
'''

# system imports
import sys
import time
import types
from multiprocessing import Process

# qpid / amqp
import qpid.messaging
import Queue

# enstore imports
import e_errors
import enstore_constants

# enstore cache imports
#from cache.messaging.client import EnQpidClient
import cache.messaging.client as cmc

debug = True

class LMD():
    '''
    classdocs
    '''

    def __init__(self, amq_broker=("localhost",5672), myaddr="lmd", target="lmd_out", auto_ack=True ):
        '''
        Constructor
        '''
        self.shutdown = False
        self.finished = False
              
        self.myaddr = myaddr
        self.target = target
        if debug: print "DEBUG lmd _init myaddr %s target %s"%(self.myaddr, self.target)
        self.qpid_client = cmc.EnQpidClient(amq_broker, self.myaddr, self.target)
        self.auto_ack = auto_ack 

    def _fetch_enstore_ticket(self):
        try:
            return self.qpid_client.rcv.fetch()
        except Queue.Empty:
            return None
        except qpid.messaging.ReceiverError, e:
            print "LMD: lmd _fetch_enstore_ticket() error: %s" % e
            return None

    def _ack_enstore_ticket(self, msg):
        try:
            if debug: print "DEBUG lmd _ack_enstore_ticket(): sending acknowledge"
            self.qpid_client.ssn.acknowledge(msg)         
        except:
            exc, emsg = sys.exc_info()[:2]
            if debug: print "DEBUG lmd _ack_enstore_ticket(): Can not send auto acknowledge for the message. Exception e=%s msg=%s" % (str(exc), str(emsg))    
            pass

##############################################################################
# Ticket Processing logic
#    
    libs = [ "9940",
             "CD-9940B",
             "CD-LTO3",
             "CD-LTO3_test",
             "CD-LTO3_test1",
             "CD-LTO4F1",
             "CD-LTO4F1T",
             "CD-LTO4G1E",
             "CD-LTO4G1T",
             "TST-9940B",
             "null1" ]

    def lmd_decision(self, ticket):
        KB=enstore_constants.KB
        MB=enstore_constants.MB
        GB=enstore_constants.GB

        result = ticket
                
        if type(ticket) != types.DictType:
            if debug: print "DEBUG lmd serve_qpid()  - ticket is not dictionary type, ticket %s." % (ticket)
            result['status'] = (e_errors.LMD_WRONG_TICKET_FORMAT, 'LMD: ticket is not dictionary type')
            return result

        try:
            # file_size_vc = ticket['file_size'] # which one?
            d = 'fc.size'
            file_size = ticket['wrapper'].get('size_bytes',0L) 
            d = 'work'
            work = ticket['work']
            d = 'vc'
            vc = ticket['vc']
            d = 'vc.library'
            library = vc['library']
            d = 'vc.file_family'
            file_family = vc['file_family']
            d = 'vc.storage_group'
            storage_group = vc['storage_group']
        except:
            if debug:
                print "DEBUG lmd serve_qpid() - encp ticket bad format, ticket %s." % (ticket)
                print "DEBUG lmd serve_qpid() d %s %s"%(type(d), d)
            
            
            result['status'] = (e_errors.LMD_WRONG_TICKET_FORMAT,"LMD: can't get required fields, %s" % d)
            return result
        #
        # Policy example. 
        # For short files redirect library to cache Library Manager
        # 
        try:
            newlib = None

            if work == 'write_to_hsm' :
                if file_size < 300*MB :
                    if library == 'CD-LTO4F1T' :
                        newlib = 'diskSF'
                    elif library == 'LTO3' : 
                        newlib = 'diskSF'
                    elif library == 'LTO5' : 
                        newlib = 'diskSF'

                elif storage_group == 'minos' :
                    newlib = 'diskSF'           

            if work == 'read_from_hsm' :
                if library == 'LTO3' :
                    newlib = 'diskSF'

            if newlib != None :
                # store original VC library in reply
                result['original_library'] = result['vc']['library']
                result['vc']['library'] = newlib
                result['vc']['wrapper'] = "null" # if file gets writtent to disk, its wrapper must be null
        except:
            exc, msg, tb = sys.exc_info()
            if debug: print "DEBUG lmd serve_qpid() - exception %s %s" % (exc, msg)
            if debug: print "DEBUG lmd serve_qpid() - newlib %s" % (newlib)
            if debug: print "DEBUG lmd serve_qpid() - result['vc']['library'] %s" % (result['vc']['library'])            
            result['status'] = (e_errors.LMD_WRONG_TICKET_FORMAT,"LMD: can't set library")

        result['status'] = (e_errors.OK, None)    
        return result

##############################################################################
        
    def serve_qpid(self):
        """
        read qpid messages from queue
        """
        self.qpid_client.start() 
        try:
            while not self.shutdown:
                # Fetch message from qpid queue
                message =  self._fetch_enstore_ticket()
                if not message:
                    continue
                if debug: print "DEBUG lmd serve_qpid()  - got encp message=%s" %(message,)

                ticket = message.content

                ##################
                # Process ticket #
                ##################
                reply = None
                try:
                    if debug: print "DEBUG lmd serve_qpid()  - received message, ticket %s." % (ticket)
                    result = self.lmd_decision(ticket)
                    if debug: print "DEBUG lmd serve_udp() : result =%s" % (result)

                    reply = qpid.messaging.Message(result, correlation_id=message.correlation_id )      
                except:
                    # @todo - report error
                    print "lmd: ERROR - can not process message, original message = %s" % (message)

                if debug: print "DEBUG lmd serve_udp() : reply =%s" % (reply)

                # send reply to encp
                try:
                    if reply :
                        self.qpid_client.send(reply)
                        if debug: print "DEBUG lmd serve_udp() : reply sent, msg=%s" % (reply)
                except qpid.messaging.SendError, e:
                    if debug: print "DEBUG lmd serve_udp() : sending reply, error=", e
                    continue
                    
                # Acknowledge ORIGINAL ticket so we will not get it again
                self._ack_enstore_ticket(message)

        # try / while 
        finally:
            self.qpid_client.stop()

    def start(self):
        # start server in separate process (we may add more processes reading the same queue)
        self.qpid_proc = Process(target=self.serve_qpid) 
        self.qpid_proc.start()                

    def stop(self):
        # tell serving thread to stop and wait until it finish    
        self.shutdown = True
        
        self.qpid_client.stop()
        self.qpid_proc.join()
        
if __name__ == "__main__":   # pragma: no cover    
    # test unit
    # instantiate LMD server
    queue_in = "udp_relay_test"
    #queue_out = "udp2amq_131.225.13.37_7700" # set it once for all messages
    queue_out = "udp2amq_131.225.13.37_7710" # set it once for all messages

    lmd = LMD(myaddr=queue_in, target=queue_out)
    lmd.start()
    
    # stop lmd server if there was keyboard interrupt
    while not lmd.finished :
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            print "Keyboard interrupt at main thread"
            lmd.stop()
            break
    
    del lmd
    print "lmd finished"
