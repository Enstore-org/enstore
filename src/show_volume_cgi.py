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
res = os.popen('enstore vol --vol '+volume).readlines()

# extract information
for i in res:
    k, v = string.split(i, ':')
    if k == " 'last_access'":
        last_access = float(v[:-2])
    if k == " 'capacity_bytes'":
        capacity = long(v[:-2])
    if k == " 'remaining_bytes'":
        remaining_bytes = long(v[:-2])
    if k == " 'system_inhibit'":
        system_inhibit = string.replace(string.replace(v[2:-3], "'", ""), ', ', '+')

print "<font size=5 color=#0000aa>"
print "<pre>"
print "          Volume:", volume
print "Last accessed on:", time.ctime(last_access)
print "      Bytes free:", show_size(remaining_bytes)
print "   Bytes written:", show_size(capacity - remaining_bytes)
print "        Inhibits:", system_inhibit
print '<hr></pre>'
print "</font><pre>"

for i in res:
    print i,
print '<p>'
print '<hr>'
res = os.popen('enstore file --list '+volume).readlines()
res = res[2:]	# get rid of header

header = "  volume         bfid              size      location cookie     status           original path"

print '<font color=#aa0000>'+header+'</font>'
print '<p>'

res.sort()
# res.sort()
for i in res:
    if string.find(i, 'active') != -1:
        print '<font color="#0000ff">',
    else:
        print '<font color="#ff0000">',
    bfid = string.split(i)[1]
    i = string.strip(i)
    i = string.replace(i, bfid, '<a href=/cgi-bin/enstore/show_file_cgi.py?bfid='+bfid+'>'+bfid+'</a>')
    print i+'</font>'

print "</body>"
print "</html>"
