import socket
import sys
import time
import StringIO

while 1:
    try:
	socket.gethostbyaddr(socket.gethostname())
	socket.gethostbyaddr("pcfar4.fnal.gov")
    except:
	format = time.strftime("%c",time.localtime(time.time()))+" "+\
		 str(sys.argv)+" "+\
		 str(sys.exc_info()[0])+" "+\
		 str(sys.exc_info()[1])+" "+\
		 "continuing"
	print format
    time.sleep(1)
