#!/usr/bin/env python

# $Id$

import os, sys
import popen2
import string
import select
import time
from fcntl import fcntl
from FCNTL import F_SETFL,O_NONBLOCK

class Gdb:
    def __init__(self, args):
        cmd = "gdb "
        for arg in args:
            cmd = cmd + " " + arg
        gdbio = popen2.popen3(cmd, 0)
        map(lambda f:fcntl(f.fileno(),F_SETFL,O_NONBLOCK), gdbio)
        self.gdb_out, self.gdb_in, self.gdb_err = gdbio
        self.allow_default_repeat = 0
        self.gdb_prompt = "(gdb) "
        
    def gdb_command(self, cmd):
        self.send_command(cmd)
        response = self.get_response()
        return response
    
    def send_command(self,cmd):
        self.gdb_in.write(cmd+"\n")
        self.gdb_in.flush()
        
    def get_response(self):
        combined_output = ""
        stdout_only = ""
        stderr_only = ""
        while 1:
            ready = select.select([self.gdb_out,self.gdb_err],[],[])
            for fd in (self.gdb_out, self.gdb_err):
                if fd in ready[0]:
                    try:
                        s = fd.read()
                    except IOError, detail:
                        if detail.errno == 11:
                            print "EAGAIN"
                            continue
                        else:
                            raise 
                    
                    if not s:
                        print "EOF?"
                        break
                
                    combined_output = combined_output+s
                    if fd == self.gdb_out:
                        stdout_only=stdout_only+s
                    elif fd == self.gdb_err:
                        stderr_only=stderr_only+s
            if stdout_only[-6:] == self.gdb_prompt:
                break
        self.response = string.split(combined_output,"\012")
        return self.response
    
    def at_gdb_breakpoint(self):
        if not self.response:
            return 0
        for line in self.response:
            if line[:len("Breakpoint ")] == "Breakpoint ":
                words = string.split(line)
                if words[1][-1]==',':
                    return string.atoi(words[1][:-1])
                else:
                    return 0
        

if __name__ == "__main__":
    def print_response(response):
        for r in response:
            if r != gdb.gdb_prompt:
                print r

    gdb = Gdb(sys.argv[1:])
    prompt = "(gdb) "

    welcome = gdb.get_response()
    print_response(welcome)
    
    while 1:
        cmd = raw_input(prompt)

        if not cmd and not gdb.allow_default_repeat:
            continue

        response = gdb.gdb_command(cmd)
        
