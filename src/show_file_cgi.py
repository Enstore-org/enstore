#!/usr/bin/env python
import cgi
import os
import string

form = cgi.FieldStorage()
bfid = form.getvalue("bfid", "unknown")
print "Content-Type: text/html"     # HTML is following
print                               # blank line, end of headers
print "<html><head><title>File "+bfid+"</title></head>"
print "<body bgcolor=#ffffd0>"
print '<pre>'

print '<h1><font color=#aa0000>', bfid, '</font></h1><p>'
# res = os.popen('enstore file --bfid '+bfid).readlines()
# res = os.popen('. /usr/local/etc/setups.sh; setup enstore; enstore file --bfid '+bfid).readlines()
res = os.popen('. /usr/local/etc/setups.sh; setup enstore; enstore info --bfid '+bfid).readlines()
for i in res:
    print i,
print "</pre></body></html>"
