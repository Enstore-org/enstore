# system imports
#
import string
import time

# enstore imports
import generic_client
import generic_cs
import backup_client
import callback
import interface
import log_client
import Trace

msg="Bloody Vikings!"

class Spam(generic_client.GenericClient):

    def __init__(self, get_logger=0):
	self.print_id = "SPAM"
	if get_logger != 0:
	    # get a logger
            self.logc = log_client.LoggerClient(0, self.print_id)

    def serve_forever(self):
	while 1:
	    self.enprint(msg)
	    time.sleep(3)
	

class SpamInterface(interface.Interface):

    def __init__(self):
        # fill in the defaults for the possible options
	self.logit1 = 0
	self.logmsg = "0"
        interface.Interface.__init__(self)

        # now parse the options
        self.parse_options()

    # define the command line options that are valid
    def options(self):
        return ["logit="]+self.help_options()

if __name__ == "__main__" :
    import sys
    Trace.init("Spam Spam Spam Spam")
    Trace.trace(1,"Spam called with args "+repr(sys.argv))

    # fill in interface
    intf = SpamInterface()

    # now get some spam
    spam = Spam(string.atoi(intf.logmsg))

    # now print the message
    spam.serve_forever()

    try:
        del spam.logc.csc.u
    except:
	pass

