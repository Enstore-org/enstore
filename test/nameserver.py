import socket
import sys
import time

while 1:
    try:
        socket.gethostbyaddr(socket.gethostname())
        socket.gethostbyaddr("pcfarm4.fnal.gov")
    except:
        format = time.strftime("%c",time.localtime(time.time()))+" "+\
                 str(sys.argv)+" "+\
                 str(sys.exc_info()[0])+" "+\
                 str(sys.exc_info()[1])+" "+\
                 "nameserver testing continuing"
        print format
    time.sleep(1)
