import socket
import sys
import time
import timeofday

while 1:
    try:
        socket.gethostbyaddr(socket.gethostname())
        socket.gethostbyaddr("pcfarm4.fnal.gov")
    except:
        format = timeofday.tod()+" "+\
                 str(sys.argv)+" "+\
                 str(sys.exc_info()[0])+" "+\
                 str(sys.exc_info()[1])+" "+\
                 "nameserver testing continuing"
        print format
    time.sleep(1)
