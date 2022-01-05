import sys
import os
import types
import db
import string
import configuration_client
import Trace

port = os.environ.get('ENSTORE_CONFIG_PORT', 0)
port = string.atoi(port)
if port:
    # we have a port
    host = os.environ.get('ENSTORE_CONFIG_HOST', 0)
    if host:
        # we have a host
        csc = configuration_client.ConfigurationClient((host, port))
    else:
        print "cannot find config host"
        sys.exit(-1)
else:
    print "cannot find config port"
    sys.exit(-1)

dbInfo = csc.get('database')
dbHome = dbInfo['db_dir']
jouHome = dbInfo['jou_dir']
print "dbHome", dbHome
print "jouHome", jouHome

vcdict = db.DbTable("volume", dbHome, jouHome, ['library', 'volume_family'])
fcdict = db.DbTable("file", dbHome, jouHome, ['external_label'])

for k in vcdict.keys():
    rec = vcdict[k]
    lib = rec['library']
    if lib in ('samm2',):
        ### del vcdict[k] ###!!!###
        print k, rec['library'], rec['non_del_files']

D=open('/home/enstore/DE','r')
for b in D.readlines():
    b=b[:-1]
    ### del fcdict[b] ###!!!###
    try:
        if fcdict[b]['deleted'] != 'yes':
            print b, fcdict[b]['deleted'], fcdict[b]['external_label'],fcdict[b]['pnfs_name0']
    except:
        print 'Trouble', fcdict[b]

#for k in dict.keys():
#    rec =  dict[k]
#    ff = rec['file_family']
#    if ff != 'none':
#        rec['volume_family'] = 'EnsV1.'+ ff
#    else:
#        rec['volume_family'] = 'null.'+ ff
#    print k, rec['volume_family']
#    dict[k] =  rec

# for k in dict.keys():
#     rec = dict[k]
#     fields = string.split(rec['volume_family'],'.')
#     size = len(fields)
#     if size == 3:
#         if fields[2] not in ('cpio_odc','null','none'):
#             print 'ERROR', k, size, fields
#     elif size > 3:
#         vf = string.join((fields[0],fields[1],fields[2]),'.')
#         rec['volume_family'] = vf
#         #dict[k] =  rec
#         print 'FUNNY:', k, size, fields, vf
#     elif size == 1:
#         print 'ONE:',k, size, fields
#     elif size == 2:
#         if fields[1] == 'none':
#             vf = string.join((fields[0],fields[1],'none'),'.')
#             rec['volume_family'] = vf
#             #dict[k] =  rec
#             print 'TWONONE:', k, size, fields, vf
#         else:
#             print 'TWO:',k, size, fields
