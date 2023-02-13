#!/usr/bin/env python

import sys
import time
import cache.messaging.client as qpid_client
import cache.messaging.pe_client as pe_client
import logging

host='dmsen04'
port=5672
fc_queue = "%s; {create: always}"%('t_fc_queue',)
pe_queue = "%s; {create: always}"%('t_pe_queue',)
TO_send = 1

if __name__ == "__main__":   # pragma: no cover

    def set_logging():
        lh = logging.StreamHandler()
        #    fmt = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        # %(pathname)s 
        fmt = logging.Formatter("%(filename)s %(lineno)d :: %(name)s :: %(module)s :: %(levelname)s :: %(message)s")
        #add formatter to lh
        lh.setFormatter(fmt)
        
        l_log = logging.getLogger('log.encache.messaging')
        l_trace = logging.getLogger('trace.encache.messaging')
        l_qpidm = logging.getLogger('qpid.messaging')

        l_log.addHandler(lh)
        l_trace.addHandler(lh)
        
        l_log.setLevel(logging.DEBUG)
        l_trace.setLevel(logging.DEBUG)

        l_qpidm.addHandler(lh)
        l_qpidm.setLevel(logging.WARNING)


    set_logging()

    c = qpid_client.EnQpidClient((host,port), fc_queue, pe_queue)
    c.start()
    print "Client:", c
    
    t = {"event":0}
    
    iev = 0
    while True:
        t['event'] = iev
        
        print 'Sending Event:', iev
        try:
            cwe = pe_client.EvtCacheWritten(t)
            c.send(cwe)
        except Exception, e:
            print "got exception ", e
            print "Exception info:" (sys.exc_info()[0:2])

        iev += 1
        time.sleep(TO_send)

