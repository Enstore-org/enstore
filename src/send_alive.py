import sys

import event_relay_client

class SendAliveInterface:

    def __init__(self):
        if len(sys.argv) < 2:
            print "ERROR: Expected name parameter"
            print "USAGE: python %s name"%(sys.argv[0],)
            self.name = None
        else:
            self.name = sys.argv[1]

def do_work(intf):
    if intf.name:
        # send an alive message
        erc = event_relay_client.EventRelayClient()
        erc.send_one_heartbeat(intf.name)
        erc.sock.close()

if __name__ == "__main__":
    # fill in interface
    intf = SendAliveInterface()

    do_work(intf)
