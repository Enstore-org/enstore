#!/usr/bin/env python

##############################################################################
#
# $Id$
# Library Mnager Director
##############################################################################


'''
    LMD - python prototype for Enstore File Cache Library Manager Dispatcher core functionality implementation
'''

# system imports
import sys
import time
import types
import string
import multiprocessing

# qpid / amqp
import qpid.messaging
import Queue

# enstore imports
import e_errors
import enstore_constants
import generic_server
import dispatching_worker
import event_relay_client
import monitored_server
import Trace
import string
import lmd_policy_selector

# enstore cache imports
#from cache.messaging.client import EnQpidClient
import cache.messaging.client as cmc

MY_NAME = enstore_constants.LM_DIRECTOR
QPID_BROKER_NAME = "amqp_broker" # this must be in enstore_constants

class LMD(dispatching_worker.DispatchingWorker,
          generic_server.GenericServer):
    '''
    classdocs
    '''

    def __init__(self, csc, auto_ack=True):
        '''
        Constructor
        '''
        generic_server.GenericServer.__init__(self, csc, MY_NAME,
                                              function = self.handle_er_msg)
        self.shutdown = False
        self.finished = False
        Trace.init(self.log_name, "yes")

        # get all necessary information from configuration
        self.lmd_config = self.csc.get(MY_NAME)
        broker_config = self.csc.get(QPID_BROKER_NAME)
        proxy_server_config = self.csc.get(self.lmd_config['udp_proxy_server'])
        
        self.queue_in = self.lmd_config['queue_in'] # this must be the same as target_addr for LMD.udp_proxy_server
        self.queue_out = "udp2amq_%s_%s"%(proxy_server_config['hostip'], proxy_server_config['udp_port']) 
        self.policy_file = self.lmd_config['policy_file']
        try:
            self.policy_selector = lmd_policy_selector.Selector(self.policy_file)
        except Exception, detail:
            Trace.log(e_errors.ALARM, "Can not create policy selector: %s" %(detail,))
            sys.exit(-1)
        
        Trace.log(e_errors.INFO, "lmd _init in %s out %s"%(self.queue_in, self.queue_out))
        #self.qpid_client = cmc.EnQpidClient(amq_broker=(broker_config['host'], broker_config['port']), self.queue_in, self.queue_out)
        
        queue_in = "%s; {create: always}"%(self.queue_in,)
        queue_out = "%s; {create: always}"%(self.queue_out,)
        self.qpid_client = cmc.EnQpidClient((broker_config['host'], broker_config['port']), queue_in, queue_out)
        self.auto_ack = auto_ack 

        self.alive_interval = monitored_server.get_alive_interval(self.csc,
                                                                  MY_NAME,
                                                                  self.lmd_config)

        dispatching_worker.DispatchingWorker.__init__(self, (self.lmd_config['hostip'],
	                                              self.lmd_config['port']))
        self.resubscribe_rate = 300

	self.erc = event_relay_client.EventRelayClient(self)
	self.erc.start_heartbeat(self.name,  self.alive_interval)

    ##############################################
    #### Configuration related methods
    ##############################################
    # reload policy when this method is called
    # by the request from the client
    def reload_policy(self, ticket):
        try:
            self.policy_selector.read_config()
            ticket['status'] = (e_errors.OK, None)
        except Exception, detail:
            ticket['status'] = (e_errors.ERROR, "Error loading policy for LMD: %s"%(detail,))
        self.reply_to_caller(ticket)
            

    # send current policy to client
    def show_policy(self, ticket):
        try:
            ticket['dump'] = self.policy_selector.policydict
            ticket['status'] = (e_errors.OK, None)
            self.send_reply_with_long_answer(ticket)
        except Exception, detail:
            ticket['status'] = (e_errors.ERROR, "Error %s"%(detail,))
            self.reply_to_caller(ticket)

    def get_library_manager(self, ticket):
        result = self.lmd_decision(ticket)
        self.reply_to_caller(result)

    def _fetch_enstore_ticket(self):
        try:
            #return self.qpid_client.rcv.fetch()
            return self.qpid_client.fetch()
        except Queue.Empty:
            return None
        except qpid.messaging.ReceiverError, e:
            Trace.trace(10, "_fetch_enstore_ticket exception %s"%(e,))
            return None

    def _ack_enstore_ticket(self, msg):
        try:
            Trace.trace(10, "lmd _ack_enstore_ticket(): sending acknowledge")
            self.qpid_client.ssn.acknowledge(msg)         
        except:
            exc, emsg = sys.exc_info()[:2]
            Trace.trace(10, "lmd _ack_enstore_ticket(): Can not send auto acknowledge for the message. Exception e=%s msg=%s" % (str(exc), str(emsg)))    
            pass

    ##############################################################################
    # Ticket Processing logic
    def lmd_decision(self, ticket):
        Trace.trace(10, "lmd_decision")         
        if type(ticket) != types.DictType:
            Trace.trace(10, "lmd serve_qpid()  - ticket is not dictionary type, ticket %s." % (ticket,))
            result['status'] = (e_errors.LMD_WRONG_TICKET_FORMAT, 'LMD: ticket is not dictionary type')
            return result
        result = ticket
        # create a copy of the original library
        result['original_library'] = result['vc']['library']
        Trace.trace(10, "lmd_decision1 %s"%(result,))         

        try:
            rc, new_library = self.policy_selector.match_found(ticket)
        except:
            exc, msg, tb = sys.exc_info()
            Trace.handle_error(exc, msg, tb)
            del(tb)
            
        Trace.trace(10, "lmd_decision2 rc=%s lm=%s"%(rc, new_library,))         
        if rc:
            result['vc']['library'] = new_library
        result['status'] = (e_errors.OK, None)    
        return result

##############################################################################
        
    def serve_qpid(self):
        """
        read qpid messages from queue
        """
        try:
            self.qpid_client.start()
        except Exception, detail:
            Trace.log(e_errors.ERROR, "Can not start qpid server: %s"%(detail,))
            sys.exit(1)
        
        try:
            while not self.shutdown:
                # Fetch message from qpid queue
                message =  self._fetch_enstore_ticket()
                if not message:
                    continue
                Trace.trace(10, "lmd serve_qpid()  - got encp message=%s" %(message,))

                ticket = message.content

                ##################
                # Process ticket #
                ##################
                reply = None
                try:
                    Trace.trace(10, "serve_qpid: ticket %s"%(ticket,))
                    result = self.lmd_decision(ticket)
                    Trace.trace(10, "serve_qpid: decision %s"%(result,))

                    reply = qpid.messaging.Message(result, correlation_id=message.correlation_id )      
                except Exception, detail:
                    Trace.log(e_errors.ERROR, "Exception processing encp request %s: %s"%(detail, message))

                # send reply to encp
                try:
                    if reply :
                        self.qpid_client.send(reply)
                        Trace.trace(10, "serve_qpid: reply sent %s"%(reply,))
                except qpid.messaging.SendError, e:
                    Trace.trace(10, "serve_qpid: exception sending reply  %s"%(e,))
                    continue
                    
                # Acknowledge ORIGINAL ticket so we will not get it again
                self._ack_enstore_ticket(message)

        # try / while 
        finally:
            self.qpid_client.stop()



    def start(self):
        #dispatching_worker.run_in_thread(thread_name="start_qpid_server",
        #                                 function=self.start_qpid_server)
        self.start_qpid_server()
        

    def start_qpid_server(self):
        # start qpid server in separate process (we may add more processes reading the same queue)
        print "START QPID SERVER"
        self.qpid_proc = multiprocessing.Process(target=self.serve_qpid) 
        self.qpid_proc.start()

    def stop_qpid_server(self):
        # tell serving thread to stop and wait until it finish    
        print "STOP QPID SERVER"
        self.shutdown = True
        
        self.qpid_client.stop()
        self.qpid_proc.join()
        sys.exit(0)
        

class LMDInterface(generic_server.GenericServerInterface):
    pass

if __name__ == "__main__":    
    # get the interface
    intf = LMDInterface()
    lmd = LMD((intf.config_host, intf.config_port))
    lmd._do_print({'levels':[10]})
    # start qpid server
    lmd.start_qpid_server()
    
    lmd.handle_generic_commands(intf)

    while 1:
        try:
            Trace.log(e_errors.INFO, "Library Manager Director (re)starting")
            lmd.serve_forever()
        except SystemExit, exit_code:
            lmd.stop_qpid_server()
            Trace.log(e_errors.INFO, "Library Manager Director Exiting %s"%(exit_code,))
            
    Trace.trace(e_errors.ERROR,"Library Manager Director finished (impossible)")

