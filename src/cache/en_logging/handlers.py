#!/usr/bin/env python

##############################################################################
#
# $Id$
##############################################################################
import logging
#import logging.config

import Trace
import e_errors

class NullHandler(logging.Handler):
    """Null handler - do not write anything"""
    def emit(self, record):
        pass

class EnLogHandler(logging.Handler):
    """ Enstore log handler - send records to enstore Trace.log facility"""
    
    def __init__(self,name):
        """Initialize the handler"""
        self.name = name
        logging.Handler.__init__(self,level=logging.DEBUG)
        #self.level = logging.DEBUG
        
        # enstore Trace name is global setting, it is called once and for all
        #Trace.init(name,'yes')
        #Trace.do_print([e_errors.INFO, e_errors.WARNING, e_errors.USER_ERROR, e_errors.ERROR, e_errors.ALARM, e_errors.EMAIL])

        #Trace.trace( 6, "str")
        #Trace.alarm(...)
        #Trace.message(...)
        #grep Trace ../../*.py | egrep -v "import Trace|Trace.init|Trace.message|Trace.log|Trace.trace|Trace.log|Trace.alarm|Trace.notify|Trace.set_logname|Trace.handle_error"    
    
    def emit(self, record, *args):
        #if record.levelno >= self.level:
        #    print "DEBUG: level, record - %s %s" % (self.level, record.levelno)
        
        ##
        # logging
        #    CRITICAL    50
        #    ERROR       40
        #    WARNING     30
        #    INFO        20
        #    DEBUG       10
        #    NOTSET       0
        #
        # e_error
        #    EMAIL      = -1  #Should this be -1???
        #    ALARM      = 0
        #    ERROR      = 1
        #    USER_ERROR = 2
        #    WARNING    = 3
        #    INFO       = 4
        #    MISC       = 5
        ##
        #            
        if record.levelno <= logging.NOTSET:
            l = e_errors.MISC
        elif record.levelno <= logging.DEBUG:
            l = e_errors.MISC
        elif record.levelno <= logging.INFO:
            l = e_errors.INFO
        elif record.levelno <= logging.WARNING:
            l = e_errors.WARNING

        # halfway between WARNING and ERROR, = 35
        elif record.levelno ==  logging.ERROR-5:  
            l = e_errors.USER_ERROR
            
        elif record.levelno <= logging.ERROR:
            l = e_errors.ERROR

        elif record.levelno <= logging.CRITICAL:
            l = e_errors.ALARM
        # severity higher then CRITICAL
        else:
            l = e_errors.EMAIL
        
        #print "DEBUG EMIT record,args:"
        #print record
        #print args
        s = self.format(record)
        Trace.log(l,s)   

class EnTraceHandler(logging.Handler):
    """ Enstore log handler - send records to enstore Trace.trace() facility
        copy-paste from EnLogHandler, only emits to Trace.trace"""
    
    def __init__(self,name):
        """Initialize the handler"""
        self.name = name
        logging.Handler.__init__(self,level=logging.DEBUG)
        #self.level = logging.DEBUG
        
        # enstore Trace name is global setting, it is called once and for all
        #Trace.init(name,'yes')
        #Trace.do_print([e_errors.WARNING, e_errors.INFO, e_errors.ERROR])

        #Trace.trace( 6, "str")
        #Trace.alarm(...)
        #Trace.message(...)
        #grep Trace ../../*.py | egrep -v "import Trace|Trace.init|Trace.message|Trace.log|Trace.trace|Trace.log|Trace.alarm|Trace.notify|Trace.set_logname|Trace.handle_error"    
    
    def emit(self, record):
        # if record.levelno >= hdlr.level:
        # print "DEBUG: level, record - %s %s" % (self.level, record.levelno)
        
        ##
        # logging
        #    CRITICAL    50
        #    ERROR       40
        #    WARNING     30
        #    INFO        20
        #    DEBUG       10
        #    NOTSET       0
        #
        # e_error
        #    EMAIL      = -1  #Should this be -1???
        #    ALARM      = 0
        #    ERROR      = 1
        #    USER_ERROR = 2
        #    WARNING    = 3
        #    INFO       = 4
        #    MISC       = 5
        ##

        #            
        if record.levelno <= logging.NOTSET:
            l = e_errors.MISC
        elif record.levelno <= logging.DEBUG:
            l = e_errors.MISC
        elif record.levelno <= logging.INFO:
            l = e_errors.INFO
        elif record.levelno <= logging.WARNING:
            l = e_errors.WARNING

        # halfway between WARNING and ERROR, = 35
        elif record.levelno ==  logging.ERROR-5:  
            l = e_errors.USER_ERROR
            
        elif record.levelno <= logging.ERROR:
            l = e_errors.ERROR

        elif record.levelno <= logging.CRITICAL:
            l = e_errors.ALARM
        # severity higher then CRITICAL
        else:
            l = e_errors.EMAIL
                           
        s = self.format(record)
        Trace.trace(l,s)   
