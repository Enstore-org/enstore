#  This file (timer_task.py) was created by Ron Rechenmacher <ron@fnal.gov> on
#  Dec  2, 1998. "TERMS AND CONDITIONS" governing this file are in the README
#  or COPYING file. If you do not have such a file, one can be obtained by
#  contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
#  $RCSfile$
#  $Revision$
#  $Date$

import time
import e_errors

timerTaskDict = {}

class TimerTask:

    def __init__( self, rcv_timeout ):
	# To become more accurate, I should override handle_request so
	# I can time how long it takes to handle the request.
	# Be careful when commands take longer than rcv_timeout.
	#self.orig_handle_request = self.handle_request
	#self.process_request = self.timerTaskProcessRequest
	self.orig_get_request = self.get_request
	self.get_request = self.timerTaskGetRequest
	self.timerTask_rcv_timeout = rcv_timeout
	self.rcv_timeout = self.timerTask_rcv_timeout
	return None

    def timerTaskGetRequest( self ):
	t0 = time.time()
	req, client_address = self.orig_get_request()
	if req == '':			# same test in handle_request
	    self.rcv_timeout = self.timerTask_rcv_timeout
	    for key in timerTaskDict.keys():
		timerTaskDict[key]['time'] = timerTaskDict[key]['time'] - self.timerTask_rcv_timeout
		if timerTaskDict[key]['time'] <= 0:
		    func = timerTaskDict[key]['func']
		    args = timerTaskDict[key]['args']
		    del timerTaskDict[key] # del first so fun can add itself back
		    apply( func, args )
	else:
	    # adjust timeout for next time
	    self.rcv_timeout = self.rcv_timeout - (time.time()-t0)
	    if self.rcv_timeout < 0: self.rcv_timeout = 0# precautionary
	    pass
	return req, client_address

    def timer_que_get( self, ticket ):
	out_ticket = {'status':(e_errors.OK,None)}
	out_ticket['que'] = repr(timerTaskDict)
	self.reply_to_caller( out_ticket )
	return



def msg_add( time, func, *args ):
    timerTaskDict[str(func)] = {'time':time,
			   'func':func,
			   'args':args}
    return None

def msg_cancel( func ):
    try: del timerTaskDict[str(func)]
    except: pass
    return None

    





