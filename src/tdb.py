import threading
import time
import socket
import string
import sys
import tdb
import linecache
import pdb
import thread
import os
import bdb

Quit = "tdb.Quit"
Help = "tdb.Help"

MODE_OFF = 0
MODE_TRACE_ALL = 1
MODE_TRACE_CALL  = 2
MODE_PDB = 3


#
# This class alters the behavoir of the PDB.  We overrrrrride teh thing that
# is seen directly by  sys.stetrace() in order to catch the bdbQuit excetion
# which is generted when the user types "Q' or Quit.  WE re-install the 
# saver.  WE still need to 
#  1) communicate this back to the telnet thread
#  2) restore stdin and stdout "back"  (right now, they are subverted
#     by the PDB item in the telnet thread.  Maybe that is not the
#     rgth place for the code.
#  3) See if >1 user can eith see teh debug session or particiapt in it,

class Hackpdb(pdb.Pdb) :
    def __init__(self):
        pdb.Pdb.__init__(self)


    def trace_dispatch(self, frame, event, arg) :
	try:
	   return bdb.Bdb.trace_dispatch(self, frame, event, arg)
	except bdb.BdbQuit:
	   pass
        # now, "all " have to do is to notify the telnet thread...
	tdb.mode = MODE_TRACE_ALL
	install()
	return None	

# WARNING VOODOO Code around:
# The python we are using when I developed this seems to have 
# a buggy stdout when we run programs in the backgronnd.
# the followng did not work:
#  python tdb.py &
#   ....
#  def pdb(...) :
#	....
#	sys.stdout = self.outFile
#       print "This is never put to the socket..."
#  interposing this Hackio instead of an obect of socket or file type 
#  "fixes" the problem. It is almost surely a bug in python, So if you are
#  off maintaining this code, try the simple case of sys.stdout = self.outFile.

class Hackio:
    outFile = sys.stdout

    def write(self, text):
        self.outFile.write(text)
        self.outFile.flush()

def saver(frame, type, u) :
    if tdb.mode is MODE_TRACE_ALL :
        tdb.simple = { 't': threading.currentThread(), 'frame' : frame }
        return saver
    elif tdb.mode is MODE_TRACE_CALL :
        tdb.simple = { 't': threading.currentThread(), 'frame' : frame }       
        return None
    elif tdb.mode is MODE_PDB :
        Hackpdb().set_trace()
        return None
    else :
       print "impossible mode"

def install():
    sys.settrace(saver)
    
tdb.mode = MODE_TRACE_ALL

def setmode(newmode):
    tdb.mode = newmode

class Tdb(threading.Thread) :
    inFile = sys.stdin
    outFile = sys.stdout

    def __init__(self):
        threading.Thread.__init__(self)

    def writeln(self, thing):
        self.outFile.write(`thing` + "\n")
        self.outFile.flush()

    def run(self) :
        self.cmd_help()
        while 1:
            try:
                self.once()
            except Quit :
                self.outFile.close()
                self.inFile.close()
                return
            except Help :
                self.cmd_help()
            except :
                import traceback
                traceback.print_exc(file=self.outFile)
            
    def once(self) :
        self.outFile.write("\ntdb>>")
        self.outFile.flush()
        line  = string.strip(self.inFile.readline())
        toks  = string.split(line)
        if  toks :
            cmd = toks[0]
            if  hasattr(self, "line_cmd_" + cmd) :
                # i.e call self.line_cmd_exec ("balance of line")
                func = getattr(self,"line_cmd_" + cmd)
                return func(string.strip(line[len(cmd):]))
            elif hasattr(self, "cmd_" + cmd) :
                #help will execute self.cmd_help()"
                func = getattr(self,"cmd_" + cmd)
                return func(toks[1:])
            else :
                raise Help

    def cmd_main(self, args) :
        if args : raise Help
        self.writeln (tdb.simple['t'])
        self.writeln("_____________________")
        f = tdb.simple['frame']
        while f :
            #self.writeln (f.__members__)
            source_desc = "->%s:(%s):%s" % (
                f.f_code.co_filename,
                repr(f.f_lineno),
                string.strip(linecache.getline(f.f_code.co_filename, f.f_lineno))
                )
            self.writeln (source_desc)
            f = f.f_back
        
    def cmd_mainwhoall(self, args) :
        if args : raise Help
        self.writeln (tdb.simple['t'])
        f = tdb.simple['frame']
        while f :
            self.writeln("_____________________")
            #self.writeln (f.__members__)
            source_desc = "->%s:(%s):%s" % (
                f.f_code.co_filename,
                repr(f.f_lineno),
                string.strip(linecache.getline(f.f_code.co_filename, f.f_lineno))
                )
            self.writeln (source_desc)
            for l in f.f_locals.keys() :
                self.writeln ("  %s=%s" % (
                    repr(l),
                    repr(f.f_locals[l]))
                    )
            f = f.f_back
        
    def cmd_list(self, args):
        if len(args) is not 2 : raise Help
        filename = args[0]
        lineno = args[1]
        for l in  range (string.atoi(lineno), string.atoi(lineno) + 10) :
            self.writeln(linecache.getline(filename + ".py", l))

    def cmd_who(self, args):
        if len(args) is not 1 : raise Help
        m = args[0]
        d = sys.modules[m].__dict__
	for e in d.keys() :
            self.writeln(repr(e))

    def cmd_whoall(self, args):
        if len(args) is not 1 : raise Help
        m = args[0]
        d = sys.modules[m].__dict__
	for e in d.keys() :
            self.writeln(repr(e)  + '=' + repr(d[e]))

    def cmd_import(self, args):
        if len(args) is not 1 : raise Help
        m = args[0]
        if 0 : print self #quiet the linter
        tdb.__dict__[m]=__import__(m)

    def line_cmd_eval(self, e):
        self.writeln(eval(e))

    def line_cmd_exec(self, e):
        if 0 : print self #linter
        exec e

    def cmd_modules(self, args):
        if len(args) is not 0 : raise Help
	for m in sys.modules.keys() :
            self.writeln(m)

    def cmd_pdb(self, args):
	h = Hackio()
	h.outFile = self.outFile
	sys.stdout = h
	sys.stdin  = self.inFile
        print "***** Quitting the debugger crashes the program** "
        print "***** This is as far as I am ** "
        tdb.setmode(MODE_PDB)

        #pdb.set_trace()
        while 1:
            time.sleep(1000)
        
    def cmd_help(self, args=()) :
        if len(args) is not 0 : raise Help
        self.writeln("help")
        self.writeln("list <filename>.py <line>-- Print 10 lines from file")
        self.writeln("who <module>             -- Print names at module scope")
        self.writeln('whoall <module>          -- Print values  "   "     "  ')
        self.writeln("import <module>          -- import a module")
        self.writeln("eval rest....            -- eval the rest of line")
        self.writeln("exec rest....            -- exec the rest of line")
        self.writeln("modules                  -- print known modules")
        self.writeln("mainwhoall               -- look at the main stack/vars")
        self.writeln("main                     -- look at the main stack")
        self.writeln("pdb                      -- pdb debugger (buggy still)")
        self.writeln("quit")
    
    def cmd_quit(self, args) :
        if 0 : print self, args # quiet the linter
        raise Quit
        
class TdbListener(threading.Thread):
    host =  "localhost"
    port = 9998
    
    def __init__(self) :
        threading.Thread.__init__(self)

    def run(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
        s.bind ((self.host,self.port))
        s.listen(2)
        while  1 :
            ns, who = s.accept()
            if 0 : print who # for the linter
            tdb = Tdb()
            tdb.inFile = ns.makefile('r')
            tdb.outFile  = ns.makefile('w')
            tdb.start()
            ns.close()

if __name__ == "__main__":

    def am_there():
        AM_THERE = 2
        time.sleep(AM_THERE)

    def am_here():
        AM_HERE = 2
        time.sleep(AM_HERE)

    def I_() :

        while 1:
            I = 1
            print "visit me at localhost 9998        !!!! "
            time.sleep(I)
            am_here()
            time.sleep(I)
            am_there()
        

    tdb.TdbListener().start()
    tdb.install()
    tdb.setmode(MODE_TRACE_CALL)
    I_()
    
