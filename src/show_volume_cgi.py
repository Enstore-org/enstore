#!/usr/bin/env python
import cgi
import enstore_utils_cgi
import os
import string
import time

kk = 1024
mm = kk * kk
gg = mm * kk

def show_size(s):
    if s > gg:
        return "%7.2f GB"%(float(s) / gg)
    elif s > mm:
        return "%7.2f MB"%(float(s) / mm)
    elif s > kk:
        return "%7.2f KB"%(float(s) / kk)
    else:
        return "%7d Bytes"%(s)

# default values
last_access = 0
capacity = 0
remaining_bytes = 0
system_inhibit = ""
   
form = cgi.FieldStorage()
volume = form.getvalue("volume", "unknown")

print "Content-Type: text/html"     # HTML is following
print                               # blank line, end of headers
print "<html>"
print "<head>"
print "<title> Volume "+volume+"</title>"
print "</head>"
print "<body bgcolor=#ffffd0>"

# setup enstore environment
config_host, config_port = enstore_utils_cgi.find_enstore()
os.environ['ENSTORE_CONFIG_HOST'] = config_host
os.environ['ENSTORE_CONFIG_PORT'] = config_port
print '<h1><font color=#aa0000>', volume, '</font></h1>'
res1 = os.popen('enstore vol --gvol '+volume).readlines()

# extract information
for i in res1:
    k, v = string.split(i, ':')
    if k == " 'last_access'":
        if int(v[-4:]) < 1970:
            la_time = '(never)'
        else:
            la_time = v
    if k == " 'capacity_bytes'":
        capacity = long(v[:-2])
    if k == " 'remaining_bytes'":
        remaining_bytes = long(v[:-2])
    if k == " 'system_inhibit'":
        si = string.split(string.replace(string.replace(v[2:-3], "'", ""), ",", ""))
        for i in [0, 1]:
            if si[i] != 'none':
                si[i] = '<font color=#ff0000>'+si[i]+'</font>'
        system_inhibit = si[0]+'+'+si[1]
        # system_inhibit = string.replace(string.replace(v[2:-3], "'", ""), ', ', '+')

fileout = []
res = os.popen('enstore file --list '+volume).readlines()
res = res[2:]	# get rid of header
total = 0L

for i in res:
    if string.find(i, 'active') != -1:
        h1 = '<font color="#0000ff">'
    else:
        h1 = '<font color="#ff0000">'
    fr = string.split(i)
    bfid = fr[1]
    total = total + long(fr[2])
    i = string.strip(i)
    i = string.replace(i, bfid, '<a href=/cgi-bin/enstore/show_file_cgi.py?bfid='+bfid+'>'+bfid+'</a>')
    fileout.append(h1+i+'</font>')

print "<font size=5 color=#0000aa><b>"
print "<pre>"
print "          Volume:", volume
print "Last accessed on:", la_time
print "      Bytes free:", show_size(remaining_bytes)
print "   Bytes written:", show_size(total)
print "        Inhibits:", system_inhibit
print '</b><hr></pre>'
print "</font><pre>"

for i in res1:
    print i,
print '<p>'
print '<hr>'

header = " volume         bfid             size      location cookie     status           original path"

print '<font color=#aa0000>'+header+'</font>'
print '<p>'

for i in fileout:
    print i

print "</body>"
print "</html>"
