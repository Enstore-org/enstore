#!/usr/bin/env python

import string

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

thefile="T"
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
     elif tokens[0] == second:
         count = count + 1
         ts = string.atol(tokens[2])%100
         tu = string.atol(tokens[4])*.000001
         t=ts+tu
         o.write("%i %f %f \n"%(count, t, (t-start)*1000000.))

 o.close()
 f.close()

# set output "output.ps"
# set terminal postscript color solid
# set log y
# plot "Trsel" using 1:3, "Tread" using 1:3, "Twsel" using 1:3, "Twrite" using 1:3
