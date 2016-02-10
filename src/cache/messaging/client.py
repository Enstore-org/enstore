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
import time
import exceptions

import qpid.messaging
import qpid.util
#from qpid.util import URL
#from qpid.log import enable, DEBUG, WARN
import cache.messaging.constants as cmsc

# qpid Connection reconnect_timeout in seconds
TO_RECONNECT_INTERVAL = 10
TO_RECONNECT_INTERVAL_MIN  = 10
TO_RECONNECT_INTERVAL_MAX = 60
TO_CON_CLOSE=5 # Connection Close timeout in sec

ALLOWED_SASL_MECHANISM=('ANONYMOUS', 'PLAIN', 'GSSAPI')
debug = False

class EnQpidError(exceptions.Exception):
    """
    Needed to raise EnQpid specific exceptions.

    """
    def __init__(self, arg):
        exceptions.Exception.__init__(self,arg)


class EnQpidClient:
    def __init__(self,
                 host_port,
                 myaddr=None,
                 target=None,
                 user=None,
                 password=None,
                 authentication='ANONYMOUS'):
        """

        :type host_port: :obj:`tuple`
        :arg host_port: (:obj:`str` - host name, :obj:`int`- port)
        :type myaddr: :obj:`str`
        :arg myaddr: queue declaration in terms of AMQP
        :type target: :obj:`str`
        :arg target: queue declaration in terms of AMQP
        :type user: :obj:`str`
        :arg user: client user name
        :type password: :obj:`str`
        :arg password: client user name
        :type authentication: :obj:`str`
        :arg authentication: space separated set of authentication mechanisms. The values can be:
        ANONYMOUS, PLAIN, CRAM-MD5, DIGEST-MD5, GSSAPI

        """

        self.user = user
        self.password = password
        self.authentication = authentication
        if self.authentication not in ALLOWED_SASL_MECHANISM:
            raise EnQpidError('Only %s machanisms are allowed'%(ALLOWED_SASL_MECHANISM))
        if self.authentication == 'PLAIN' and self.user is None:
            self.user = 'guest'
            self.password = 'guest'

        self.log = logging.getLogger('log.encache.messaging')
        self.trace = logging.getLogger('trace.encache.messaging')

        self.host, self.port = host_port

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
        self.trace.debug("EnQpidClient broker host, port: %s %s", self.host, self.port )

        self.conn = qpid.messaging.Connection(host=self.host,
                                              port=self.port,
                                              username=self.user,
                                              password=self.password,
                                              sasl_mechanisms=self.authentication,
                                              reconnect=True,
                                              reconnect_interval=TO_RECONNECT_INTERVAL)

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
            self.conn.close(TO_CON_CLOSE)
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


if __name__ == "__main__":
    import time
    import optparse

    user = password = None
    parser = optparse.OptionParser()
    parser.add_option("--sasl-mechanism", action="store", type="string", metavar="<mech>", help="SASL mechanism for authentication (ANONYMOUS, PLAIN, GSSAPI)")
    opts, encArgs = parser.parse_args(args=sys.argv)
    if not opts.sasl_mechanism:
        auth = 'ANONYMOUS'
    else:
        auth = opts.sasl_mechanism
    if auth not in ALLOWED_SASL_MECHANISM:
        print "only %s is allowed"%(ALLOWED_SASL_MECHANISM)
        sys.exit(1)
    if auth in ('ANONYMOUS', 'PLAIN'):
        user = 'guest'
        password = 'guest'

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

    amq_broker=("dmsen05",5672)
    myaddr="t_policy_engine; {create: always, delete: always}"
    target="t_migration_dispatcher; {create: always, delete: always}"
    qr = "t_md_replies; {create: always, delete: always}"
    qm = "t_migrator; {create: always, delete: always}"

    c = EnQpidClient(amq_broker, myaddr, target=target,
                     user=user, password=password, authentication=auth)
    #c = EnQpidClient(amq_broker, myaddr=myaddr, target=None)
    #c = EnQpidClient(amq_broker, None, target=target)

    print c
    c.start()
    print c

    r = c.add_receiver("from_md",qr)
    s = c.add_sender("to_mg",qm, durable=True) # some existing queue
    print r
    print s
    print c

    do_fetch = False
    do_send = True
    do_consume = True

    if do_fetch:
        m = c.fetch()
        if m :
            print m
            # ack message, one way of tree below:
            #c.ssn.acknowledge()                # ack all messages in the session
            c.ssn.acknowledge(m)                # ack this message
            #c.ssn.acknowledge(m,sync=False)    # ack this message, do not wait till ack is consumed

    print "To interrupt press ^C"
    while 1:
        try:
            if do_send:
                c.send("client2 unit test")

            # consume message we just sent
            rdr = c.add_receiver("drain",target)
            time.sleep(1)

            mr=c.drain.fetch()
            c.ssn.acknowledge(mr)
            print time.ctime()
            print mr
        except (SystemExit, KeyboardInterrupt):
            break
        except Exception, detail:
            print detail
            break
