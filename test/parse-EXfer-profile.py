#!/usr/bin/env python

import string
import sys
import time

# the next line causes the program to traceback and exit if it is not called properly
thefile=sys.argv[1]
begin=-1
end=-1


for kind in "rsel","read","wsel","write":
 print kind

 if kind == "rsel":
    first  = "1:"
    second = "2:"

 elif kind == "read":
    first  = "3:"
    second = "4:"

 if kind == "wsel":
    first  = "5:"
    second = "6:"

 elif kind == "write":
    first  = "7:"
    second = "8:"

 f = open(thefile,"r")
 o = open(thefile+kind,"w")

 count=0
 start=0

 while 1:
     line = f.readline()
     if not line: break
     tokens = line.split()
     if tokens[0] == first:
         ts = string.atol(tokens[2])%100
         tu = string.atol(tokens[4])*.000001
         start=ts+tu
         if begin == -1:
             begin=string.atol(tokens[2])+string.atol(tokens[4])*.000001
     elif tokens[0] == second:
         count = count + 1
         ts = string.atol(tokens[2])%100
         tu = string.atol(tokens[4])*.000001
         t=ts+tu
         s=tokens[2]
         u=tokens[4]
         delta=(t-start)*1000000.
         bytes=string.atol(tokens[5])
         Mbytespersec=bytes/delta/1024./1024.*1000000
         o.write("%i %f %f %f %f \n"%(count, t, delta, bytes,Mbytespersec))

 if end==-1:
     end=string.atol(s)+string.atol(u)*.000001
 o.close()
 f.close()

print '%s from %s to %s'%(thefile,time.ctime(begin),time.ctime(end))
print 'gnuplot'
print ' set output "output.ps"'
print ' set terminal postscript color solid'
print ' set log y'
print ' plot "%srsel" using 1:3, "%sread" using 1:3, "%swsel" using 1:3, "%swrite" using 1:3'%(thefile,thefile,thefile,thefile)
print ' plot "%sread" using 1:5, "%swrite" using 1:5'%(thefile,thefile)
print ' plot "%sread" using 4:5, "%swrite" using 4:5'%(thefile,thefile)
