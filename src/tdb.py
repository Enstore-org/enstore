import threading
import time
import socket
import string
import sys
import tdb
import linecache
import __main__

Quit = "Quit"


def saver(frame, type, u) :
    if 0:
        print ("attention, in saver",  frame, type, u, __main__.on,
               linecache.getline(frame.f_code.co_filename, frame.f_lineno))

    if not __main__.on :
        return saver
    __main__.simple = { 't'     : threading.currentThread(),
                               'frame' : frame
                              }
    return saver

def dumper():
    #print __main__.on
    #print __main__.simple
    #print "dumper" , type(__main__.simple['t'])
    #print "dumper" , __main__.simple['frame'].__members__
    pass
    return

def install():
    sys.settrace(saver)
    
__main__.on = 0

def onoff(which):
    __main__.on = which


class Tdb(threading.Thread) :
    inFile = sys.stdin
    outFile = sys.stdout

    def __init__(self):
        threading.Thread.__init__(self)

    def writeln(self, thing):
        self.outFile.write(`thing` + "\n")
        self.outFile.flush()

    def run(self) :
        self.help()
        while 1:
            try:
                self.once()
            except Quit :
                self.outFile.close()
                self.inFile.close()
                return
            except :
                import traceback
                traceback.print_exc(file=self.outFile)
                self.help()
            
    def once(self) :
        self.outFile.write("\ntdb>>")
        self.outFile.flush()

        line  = string.strip(self.inFile.readline())
        toks  = string.split(line)
        if  toks :
            if toks[0]  ==  "list" :
                if len(toks) == 3 :
                    self.list(toks[1],toks[2])
                else :
                    self.help()
            elif toks[0] ==   "who" :
                self.who(string.strip(line[3:]))
            elif toks[0] ==   "whoall" :
                self.whoall(string.strip(line[6:]))
            elif toks[0] ==   "import" :
                self.imprt(string.strip(line[6:]))
            elif toks[0] == "eval" :
                self.eval(line[4:])
            elif toks[0] == "exec" :
                self.exc(string.strip(line[4:]))
            elif toks[0] == "help":
                self.help()
            elif toks[0] == "modules":
                self.modules()
            elif toks[0] == "mainwhoall":
                self.mainwhoall()
            elif toks[0] == "main":
                self.main()
            elif toks[0] ==  "quit" :
                self.quit()
            elif toks[0] ==  "teardown" :
                self.teardown()
            else :
                self.help()
                
    def main(self) :
        self.writeln (__main__.simple['t'])
        self.writeln("_____________________")
        #self.writeln (__main__.simple['frame'].__members__)
        f = __main__.simple['frame']
        while f :
            #self.writeln (f.__members__)
            source_desc = " %19s %4s:%50s" % (
                f.f_code.co_filename[-20:],
                repr(f.f_lineno),
                repr(linecache.getline(f.f_code.co_filename, f.f_lineno))
                )
            self.writeln (source_desc)
            f = f.f_back
        
    def mainwhoall(self) :
        self.writeln (__main__.simple['t'])
        #self.writeln (__main__.simple['frame'].__members__)
        f = __main__.simple['frame']
        while f :
            self.writeln("_____________________")
            #self.writeln (f.__members__)
            source_desc = " %19s %4s:%50s" % (
                 f.f_code.co_filename[-20:],
                 repr(f.f_lineno),
                 repr(linecache.getline(f.f_code.co_filename, f.f_lineno))
                 )
            self.writeln (source_desc)
            for l in f.f_locals.keys() :
                self.writeln ("  %s=%s" % (
                    repr(l),
                    repr(f.f_locals[l]))
                    )
            f = f.f_back
        
    def list(self, filename, lineno):
        import linecache
        for l in  range (string.atoi(lineno), string.atoi(lineno) + 10) :
            self.writeln(linecache.getline(filename + ".py", l))

    def who(self, m):
        d = sys.modules[m].__dict__
	for e in d.keys() :
            self.writeln(repr(e))

    def whoall(self, m):
        d = sys.modules[m].__dict__
	for e in d.keys() :
            self.writeln(repr(e)  + '=' + repr(d[e]))

    def imprt(self, m):
        if 0 : print self #quiet the linter
        tdb.__dict__[m]=__import__(m)

    def eval(self, e):
        self.writeln(eval(e))

    def exc(self, e):
        if 0 : print self #linter
        exec e

    def modules(self):
	for m in sys.modules.keys() :
            self.writeln(m)

    def teardown(self):
        if 0 :
            me = threading.currentThread()
            for t in threading.enumerate():
                if t is not me:
                    t.exit()
            me.exit()
        else :
            self.writeln("No yet working")
    

    def help(self):
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
        self.writeln("teardown                 -- teardown the whole app")
        self.writeln("quit")
    
    def quit(self):
        if 0 : print self # quiet the linter
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
    install()
    onoff(1)
    I_()
    
        






