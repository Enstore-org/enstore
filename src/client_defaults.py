###############################################################################
# src/$RCSfile$   $Revision$
#
class GenericClient ():

    # check on alive status
    def alive(self, rcv_timeout=0, tries=0):
        return self.send({'work':'alive'}, rcv_timeout, tries)
