import threading
import time
import  socket
import string
import sys
Quit = "Quit"

class Tdb(threading.Thread) :
    inFile = sys.stdin
    outFile = sys.stdout
    
    def __init__(self):
        threading.Thread.__init__(self)

    def writeln(self, thing):
        self.outFile.write(`thing`+ "\n")
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
            elif toks[0] == "eval" :
                self.eval(line[4:])
            elif toks[0] == "exec" :
                self.exc(line[4:])
            elif toks[0] == "help":
                self.help()
            elif toks[0] == "modules":
                self.modules()
            elif toks[0] ==  "quit" :
                self.quit()
            else :
                self.help()

    def list(self, filename, lineno):
        import linecache
        for l in  range (string.atoi(lineno), string.atoi(lineno) + 10) :
            self.writeln(linecache.getline(filename, l))

    def who(self, m):
        d = sys.modules[m].__dict__
	for e in d.keys() :
            self.writeln(repr(e)  + '=' + repr(d[e]))

    def eval(self, e):
        self.writeln(eval(e))

    def exc(self, e):
        if 0 : print e #linter
        self.writeln("not yet there")

    def modules(self):
	for m in sys.modules.keys() :
            self.writeln(m)

    def  help(self):
        self.writeln(
	  "help,list <filename> <line>, who <module>, eval <expression>, modules, quit")
    
    def quit(self):
        if 0 : print self # quiet the linter
        raise Quit
        
class TdbListener(threading.Thread):
    host =  "localhost"
    port = 9999
    
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
        
if __name__ == "__main__" :
    dog = {'cat' : 1} 
    TdbListener().start()
    while 1:
        print "visit me at localhost 9999        !!!! ", dog
        time.sleep(10)

