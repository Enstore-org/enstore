#!/usr/bin/env python
import cgi
import enstore_utils_cgi
import os
import string

form = cgi.FieldStorage()
bfid = form.getvalue("bfid", "unknown")
print "Content-Type: text/html"     # HTML is following
print                               # blank line, end of headers
print "<html><head><title>File "+bfid+"</title></head>"
print "<body bgcolor=#ffffd0>"
print '<pre>'

# setup enstore environment
config_host, config_port = enstore_utils_cgi.find_enstore()
os.environ['ENSTORE_CONFIG_HOST'] = config_host
os.environ['ENSTORE_CONFIG_PORT'] = config_port
print '<h1><font color=#aa0000>', bfid, '</font></h1><p>'
res = os.popen('enstore file --bfid '+bfid).readlines()
for i in res:
    print i,
print "</pre></body></html>"
