#!/usr/bin/env python

import os
import sys
import string
import socket

inv_dir = "/enstore/tape_inventory"

host = string.split(socket.gethostname(), '.')[0]
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

special = ['TOTAL_BYTES_ON_TAPE', 'VOLUMES', 'VOLUMES_DEFINED.html', 'VOLUMES_TOO_MANY_MOUNTS', 'VOLUME_QUOTAS', 'VOLUME_SIZE', 'LAST_ACCESS', 'NOACCESS', 'CLEANING', 'VOLUME_CROSS_CHECK', 'COMPLETE_FILE_LISTING', 'DECLARATION_ERROR', 'MIGRATED_VOLUMES', 'MIGRATION_STATUS', 'RECYCLABLE_VOLUMES', 'QUOTA_ALERT']

if cluster == "d0en":
	special.append('AML2-VOLUMES.html')
elif cluster == "stken":
	special.append('VOLUME_QUOTAS_UPDATE')
	special.append('AMLJ-VOLUMES.html')
	special.append('STK-VOLUMES.html')
elif cluster == "cdfen":
	special.append('VOLUME_QUOTAS_UPDATE')
	special.append('STK-VOLUMES.html')

# in the beginning ...

print "Content-type: text/html"
print

# taking care of the header

print '<html>'
print '<head>'
print '<title>Tape Inventory Summary on '+cluster+'</title>'
print '</head>'
print '<body bgcolor="#ffffd0">'
print '<font size=7 color="#ff0000">Enstore Tape Inventory Summary on '+cluster+'</font>'
print '<hr>'

# handle special files

print '<p><font size=4>'
for i in special:
	print '<a href="'+os.path.join(inv_dir, i)+'">', string.split(i, '.')[0], '</a>&nbsp;&nbsp;'
	print '<br>'
print '<p><a href="'+inv_dir+'">Raw Directory Listing</a>'
print '<p></font>'
print '</body>'
print '</html>'
