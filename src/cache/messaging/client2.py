#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################
'''
client.py - Enstore qpid messaging client
'''
import logging
import sys

import qpid.messaging
import qpid.util
#from qpid.util import URL
#from qpid.log import enable, DEBUG, WARN
import cache.messaging.constants as cmsc

debug = False

class EnQpidClient:
    def __init__(self, host_port, myaddr=None, target=None ):
        self.log = logging.getLogger('log.encache.messaging')
        self.trace = logging.getLogger('trace.encache.messaging')
        
        # skip all above, new:
        # @todo: url (and connection.url) keeps password in open when set, thus it is meaningless
        #   connection MUST be used over ssl
        (h, p) = host_port
        u = "guest/guest@%s:%s" % (h,p,)
        self.broker = qpid.util.URL(u)

        self.trace.debug("EnQpidClient URL init: %s %s ", self.broker.host, self.broker.port )
                
        if myaddr is not None:
            self.myaddr = myaddr     # my address to be used in reply receiver
        if target is not None:
            self.target = target    # destination address to be used in sender
        
    def __str__(self):
        # show all variables in sorted order
        showList = sorted(set(self.__dict__))

        return ("<%s instance at 0x%x>:\n" % (self.__class__.__name__,id(self))) + "\n".join(["  %s: %s" 
                % (key.rjust(8), self.__dict__[key]) for key in showList])
        
    def start(self):
        # @todo
        # print "DEBUG EnQpidClient URL start:" + self.url
        self.trace.debug("EnQpidClient broker host, port: %s %s", self.broker.host, self.broker.port )

        self.conn = qpid.messaging.Connection(url=self.broker)
        self.conn.reconnect = True

        self.conn.open()
        self.ssn = self.conn.session()

        try:
            self.snd_default = self.ssn.sender(self.target)    # default sender sends messages to target
        except qpid.messaging.MessagingError:
            self.log.error("EnQpidClient - MessagingError, exception %s %s", sys.exc_info()[0], sys.exc_info()[1] )
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except AttributeError:
            self.trace.debug("EnQpidClient - no 'target' defined")
        except:
            self.trace.exception("EnQpidClient - getting sender")
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

        try:
            if not self.myaddr:
                # set reply queue
                #create exclusive queue with unique name for replies
                self.myaddr = "reply_to:" + self.ssn.name             
                self.ssn.queue_declare(queue=self.myaddr, exclusive=True)
                # @todo fix exchange name
                self.ssn.exchange_bind(exchange="enstore.fcache", queue=self.myaddr, binding_key=self.myaddr)
            #else:
            # do nothing - assume queue exists and bound, or the address contain option to create queue

            self.rcv_default = self.ssn.receiver(self.myaddr) # default receiver receives messages sent to us at myaddr
            
        except AttributeError:
            self.trace.debug("EnQpidClient - no 'myaddr' defined")
            pass

    def stop(self):
        # @todo: We do not acknowledge whatever is left in the queue - it is not processed.
        self.started = False
        try:
            self.conn.close()
        except :
            self.log.exception("qpid client - Can not close qpid connection ", self.conn)

    # @todo block/async
    def send(self, msg, *args, **kwargs ):
        try:
            self.snd_default.send(msg, *args, **kwargs )
        except:
            self.log.exception("qpid client send()")
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
          
    def fetch(self, *args, **kwargs ):
        try:
            return self.rcv_default.fetch(*args, **kwargs )
        except qpid.messaging.LinkClosed:
            self.log.exception("qpid client fetch - LinkClosed" )
        except:
            self.log.exception("qpid client fetch()")
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

    # this will work only after client is started (session must be set prior this call)
    def add_receiver(self,name,source,**options):
        """
        create additional receiver to read "source" queue
        """
        rec = self.ssn.receiver(source,**options) 
        setattr(self, name, rec)
        return getattr(self,name)

    # this will work only after client is started (session need to be set)
    def add_sender(self, name,target,**options):
        """
        create additional sender 'target' to which messages will be sent 
        """
        snd = self.ssn.sender(target,**options) 
        setattr(self, name, snd)
        return getattr(self,name)

        
if __name__ == "__main__":   # pragma: no cover
    
    def set_logging():
        lh = logging.StreamHandler()
        #    fmt = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        # %(pathname)s 
        fmt = logging.Formatter("%(filename)s %(lineno)d :: %(name)s :: %(module)s :: %(levelname)s :: %(message)s")
        
        l_log = logging.getLogger('log.encache.messaging')
        l_trace = logging.getLogger('trace.encache.messaging')
        #add formatter to lh
        lh.setFormatter(fmt)
        l_log.addHandler(lh)
        l_trace.addHandler(lh)
        
        l_log.setLevel(logging.DEBUG)
        l_trace.setLevel(logging.DEBUG)
    
    set_logging()
    
    amq_broker=("dmsen06.fnal.gov",5672)
    myaddr="policy_engine"
    target="migration_dispatcher"

    c = EnQpidClient(amq_broker, myaddr, target=target)
    #c = EnQpidClient(amq_broker, myaddr=myaddr, target=None)
    #c = EnQpidClient(amq_broker, None, target=target)
    
    print c
    c.start()
    print c
    
    r = c.add_receiver("from_md","md_replies")
    s = c.add_sender("to_mg","migrator", durable=True) # some existing queue
    print r
    print s
    print c 
    
    do_fetch = False
    do_send = True
    
    if do_fetch:
        m = c.fetch()
        if m :
            print m
            # ack message, one way of tree below:
            #c.ssn.acknowledge()                # ack all messages in the session
            c.ssn.acknowledge(m)                # ack this message
            #c.ssn.acknowledge(m,sync=False)    # ack this message, do not wait till ack is consumed 

    if do_send:
        c.send("client2 unit test")
