#!/usr/bin/env python

from __future__ import print_function
import os
import sys
import stat
import pprint
import socket

http_inv_dir = "/enstore/tape_inventory"  # httpd path
# This is a kind of tricky
# get DOCUMENT_ROOT yet get rid of first "/" in http_inv_dir
inv_dir = os.path.join(os.environ['DOCUMENT_ROOT'], http_inv_dir[1:])

host = socket.gethostname().split('.')[0]
if host[:3] == "rip":
    cluster = "rip"
elif host[:3] == "stk":
    cluster = "stken"
elif host[:3] == "d0e":
    cluster = "d0en"
elif host[:3] == "cdf":
    cluster = "cdfen"
else:
    cluster = "unknown"

listing = []
for i in os.listdir(inv_dir):
    if i.startswith("RECENT_FILES_ON_TAPE"):
        p = os.path.join(inv_dir, i)
        st = os.stat(p)
        size = st[stat.ST_SIZE]
        f = open(p)
        mt = ' '.join(f.readline().split()[3:])
        f.close()
        listing.append((i, size, mt))

# in the beginning ...

print("Content-type: text/html")
print()

# taking care of the header

print('<html>')
print('<head>')
print('<title>RECENT FILES ON TAPE LISTING</title>')
print('</head>')
print('<body bgcolor="#ffffd0">')
print(
    '<font size=7 color="#ff0000">Complete File Listing on ' +
    cluster +
    '</font>')
print('<br><br>')
print('<font size=5 color="#ff0000"><b><i><blink>WARNING!</blink></i></b></font><br>')
print('<font size=4 color="#ff0000"><i>')
print('Many of the files are huge. Do not view them directly in the browser.<br>Use "Save Link Target As..." (right-click on the links) to save them to your local file system')
print('</i>')
print('</font>')
print('<br><br>')
print('The following files are tab-delimited.')
print('<br><br>')
print('<table border="1">')
print('<th>Storage Group<th>Time<th>File Listing<th>Size<tr>')
for i in listing:
    sg = i[0].split('_')[-1]
    if sg == "LISTING" or sg == "ALL":
        sg = ""
    print('<td>%s<td>%s<td><a href=%s>%s</a><td align=right>%d<tr>' %
          (sg, i[2], os.path.join(http_inv_dir, i[0]), i[0], i[1]))
print('</table>')
print('</body>')
print('</html>')
