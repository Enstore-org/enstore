#!/usr/bin/env python

##############################################################################
#
# $Id$
# Library Mnager Director
##############################################################################


'''
    LMD - python prototype for Enstore File Cache Library Manager Dispatcher core functionality implementation
'''
from __future__ import print_function

# system imports
import sys
import time
import types
import string
#import multiprocessing
import threading

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

MY_NAME = enstore_constants.LM_DIRECTOR
QPID_BROKER_NAME = "amqp_broker"  # this must be in enstore_constants


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
                                              function=self.handle_er_msg)
        self.shutdown = False
        self.finished = False
        Trace.init(self.log_name, "yes")

        # get all necessary information from configuration
        self.lmd_config = self.csc.get(MY_NAME)
        udp_proxy_server = self.lmd_config.get('udp_proxy_server', None)
        Trace.log(e_errors.INFO, "UDP proxy server: %s" % (udp_proxy_server,))
        self.proxy_server_config = None
        if udp_proxy_server:
            # UDP proxy server is defined
            # use qpid
            self.qpid = __import__("qpid")
            self.Queue = __import__("Queue")
            import cache.messaging.client as cmc
            broker_config = self.csc.get(QPID_BROKER_NAME)
            self.proxy_server_config = self.csc.get(udp_proxy_server, None)
            Trace.log(
                e_errors.INFO, "UDP proxy server conf: %s" %
                (self.proxy_server_config,))

            # this must be the same as target_addr for LMD.udp_proxy_server
            self.queue_in = self.lmd_config['queue_in']
            self.queue_out = "udp2amq_%s_%s" % (
                self.proxy_server_config['hostip'], self.proxy_server_config['udp_port'])
            queue_in = "%s; {create: always}" % (self.queue_in,)
            queue_out = "%s; {create: always}" % (self.queue_out,)
            self.qpid_client = cmc.EnQpidClient(
                (broker_config['host'], broker_config['port']), queue_in, queue_out)
            self.auto_ack = auto_ack
            Trace.log(
                e_errors.INFO, "lmd _init in %s out %s" %
                (self.queue_in, self.queue_out))

        self.policy_file = self.lmd_config['policy_file']
        try:
            self.policy_selector = lmd_policy_selector.Selector(
                self.policy_file)
        except Exception as detail:
            Trace.log(
                e_errors.ALARM, "Can not create policy selector: %s" %
                (detail,))
            sys.exit(-1)

        self.alive_interval = monitored_server.get_alive_interval(self.csc,
                                                                  MY_NAME,
                                                                  self.lmd_config)

        dispatching_worker.DispatchingWorker.__init__(self, (self.lmd_config['hostip'],
                                                             self.lmd_config['port']))
        self.resubscribe_rate = 300
        self.erc = event_relay_client.EventRelayClient(self)
        self.erc.start_heartbeat(self.name, self.alive_interval)

    ##############################################
    # Configuration related methods
    ##############################################
    # reload policy when this method is called
    # by the request from the client
    def reload_policy(self, ticket):
        try:
            self.policy_selector.read_config()
            ticket['status'] = (e_errors.OK, None)
        except Exception as detail:
            ticket['status'] = (
                e_errors.ERROR, "Error loading policy for LMD: %s" %
                (detail,))
        self.reply_to_caller(ticket)

    # send current policy to client

    def show_policy(self, ticket):
        try:
            ticket['dump'] = self.policy_selector.policydict
            ticket['status'] = (e_errors.OK, None)
            self.send_reply_with_long_answer(ticket)
        except Exception as detail:
            ticket['status'] = (e_errors.ERROR, "Error %s" % (detail,))
            self.reply_to_caller(ticket)

    def get_library_manager(self, ticket):
        result = self.lmd_decision(ticket)
        self.reply_to_caller(result)

    def _fetch_enstore_ticket(self):
        try:
            # return self.qpid_client.rcv.fetch()
            return self.qpid_client.fetch()
        except self.Queue.Empty:
            return None
        except self.qpid.messaging.ReceiverError as e:
            Trace.trace(10, "_fetch_enstore_ticket exception %s" % (e,))
            return None

    def _ack_enstore_ticket(self, msg):
        try:
            Trace.trace(10, "lmd _ack_enstore_ticket(): sending acknowledge")
            self.qpid_client.ssn.acknowledge(msg)
        except BaseException:
            exc, emsg = sys.exc_info()[:2]
            Trace.trace(
                10, "lmd _ack_enstore_ticket(): Can not send auto acknowledge for the message. Exception e=%s msg=%s" %
                (str(exc), str(emsg)))
            pass

    ##########################################################################
    # Ticket Processing logic
    def lmd_decision(self, ticket):
        Trace.trace(10, "lmd_decision")
        if not isinstance(ticket, dict):
            Trace.trace(
                10, "lmd_decision  - ticket is not dictionary type, ticket %s." %
                (ticket,))
            return {'status': (e_errors.LMD_WRONG_TICKET_FORMAT,
                               'LMD: ticket is not dictionary type')}
        result = ticket
        # create a copy of the original library
        try:
            result['original_library'] = result['vc']['library']
        except KeyError:
            result['status'] = (e_errors.MALFORMED, "No library key specified")
            return result
        Trace.trace(10, "lmd_decision1 %s" % (result,))

        try:
            rc, new_library = self.policy_selector.match_found(ticket)
        except BaseException:
            exc, msg, tb = sys.exc_info()
            Trace.handle_error(exc, msg, tb)
            del(tb)

        Trace.trace(10, "lmd_decision2 rc=%s lm=%s" % (rc, new_library,))
        if rc:
            result['vc']['library'] = new_library
            # do not allow multiple copies if request was re-directed
            # the copy will be done on a package file
            copies = result['fc'].get("copies", 0)
            if copies:
                result['fc']['copies'] = 0
        result['status'] = (e_errors.OK, None)
        return result

##############################################################################

    def serve_qpid(self):
        """
        read qpid messages from queue
        """
        try:
            self.qpid_client.start()
        except Exception as detail:
            Trace.log(
                e_errors.ERROR, "Can not start qpid server: %s" %
                (detail,))
            sys.exit(1)

        try:
            while not self.shutdown:
                # Fetch message from qpid queue
                message = self._fetch_enstore_ticket()
                if not message:
                    continue
                Trace.trace(
                    10, "lmd serve_qpid()  - got encp message=%s" %
                    (message,))

                ticket = message.content

                ##################
                # Process ticket #
                ##################
                reply = None
                try:
                    Trace.trace(10, "serve_qpid: ticket %s" % (ticket,))
                    result = self.lmd_decision(ticket)
                    Trace.trace(10, "serve_qpid: decision %s" % (result,))

                    reply = self.qpid.messaging.Message(
                        result, correlation_id=message.correlation_id)
                except Exception as detail:
                    Trace.log(
                        e_errors.ERROR, "Exception processing encp request %s: %s" %
                        (detail, message))

                # send reply to encp
                try:
                    if reply:
                        self.qpid_client.send(reply)
                        Trace.trace(10, "serve_qpid: reply sent %s" % (reply,))
                except self.qpid.messaging.SendError as e:
                    Trace.trace(
                        10, "serve_qpid: exception sending reply  %s" %
                        (e,))
                    continue

                # Acknowledge ORIGINAL ticket so we will not get it again
                self._ack_enstore_ticket(message)

        # try / while
        finally:
            self.qpid_client.stop()

    def start(self):
        # dispatching_worker.run_in_thread(thread_name="start_qpid_server",
        #                                 function=self.start_qpid_server)
        self.start_qpid_server()

    def start_qpid_server(self):
        if not self.proxy_server_config:
            return
        # start qpid server in separate process (we may add more processes
        # reading the same queue)
        print("START QPID SERVER")
        # Use threading!
        # For multiprocessing need to make special arrangements for
        # object which are suppesed to be shared.
        self.qpid_proc = threading.Thread(target=self.serve_qpid)
        self.qpid_proc.start()

    def stop_qpid_server(self):
        if not self.proxy_server_config:
            return
        # tell serving thread to stop and wait until it finish
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
    # lmd._do_print({'levels':[10]})
    # start qpid server
    lmd.start_qpid_server()

    lmd.handle_generic_commands(intf)

    while True:
        try:
            Trace.log(e_errors.INFO, "Library Manager Director (re)starting")
            lmd.serve_forever()
        except SystemExit as exit_code:
            lmd.stop_qpid_server()
            Trace.log(
                e_errors.INFO, "Library Manager Director Exiting %s" %
                (exit_code,))

    Trace.trace(
        e_errors.ERROR,
        "Library Manager Director finished (impossible)")
