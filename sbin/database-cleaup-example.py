#!/usr/bin/env python

# example of how to clean databases of garbage

import db
import string

f=db.DbTable('file','/diskb/enstore-database','/diska/enstore-journal')

for bfid in f.keys():
 if string.find(f[bfid]['external_label'],'sam') == 0:
   print bfid, f[bfid]['external_label']
   del f[bfid]


v=db.DbTable('volume','/diskb/enstore-database','/diska/enstore-journal')

for label in v.keys():
 if string.find(label,'sam') == 0:
   print label
   del v[label]

