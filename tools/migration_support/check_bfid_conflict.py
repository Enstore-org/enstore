#!/usr/bin/env python
import sys
import errno
import configuration_client
import info_client
import file_clerk_client
import e_errors
import namespace
import chimera
import checksum
import pg

csc = configuration_client.ConfigurationClient(('stkensrv10n', 7500))
ic_conf = csc.get("info_server")
infoc = info_client.infoClient(csc,
                               server_address=(ic_conf['host'],
                                               ic_conf['port']))
fc_conf = csc.get("file_clerk")
fcc = file_clerk_client.FileClient(csc, bfid=0,
                                   server_address=(fc_conf['host'],
                                                   fc_conf['port']))
db_info = csc.get('database')
dbhost = db_info['dbhost']
dbport = db_info['dbport']
dbname = db_info['dbname']
dbuser = db_info['dbuser']
db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)
ns = namespace.StorageFS('/pnfs/fs', '/pnfs/fs')

with open(sys.argv[1], 'r') as fil:
    while True:
        l = fil.readline()
        if not l:
            break
        bfid = l[:-1]
        print('Checking', bfid)
        f_info = infoc.find_same_file(bfid)
        #print("FINFO", f_info)
        if not e_errors.is_ok(f_info):
            print('Error getting info for', bfid, ' :', f_info['status'])
            continue
        for f in f_info['files']:
            state = 'unknown'
            if f['deleted'] == 'yes':
                state = 'deleted'
            elif f['deleted'] == 'no':
                state = 'active'
                
            print(f['external_label'], 
                  f['bfid'],
                  f['size'],
                  f['complete_crc'],
                  f['location_cookie'],
                  state,
                  f['pnfsid'],
                  f['pnfs_name0']
                  )
        pnfs_path = raw_input('pnfs path to check: ')
        l4 =  chimera.get_layer_4(pnfs_path)
        if not l4:
            continue
        print('Pnfs info')
        for key, val in l4.iteritems():
            print(key, val)
        s_q = "select * from migration where src_bfid='%s';"%(bfid,)
        #print('sq', s_q)
        try:
            res = db.query(s_q).dictresult()
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            print('select exception: %s %s'%(str(exc_type), str(exc_value)))
        #print('Select returned %s'%(res,))
        if len(res) != 0:
            for r in res:
                print(r)
        delete_bfid = raw_input('delete bfid: ')
        if delete_bfid:
            res = fcc.set_deleted('yes', bfid = delete_bfid)
            if not e_errors.is_ok(res):
                print('set_deleted returned', res)
            else:
                d_q = "delete from migration where dst_bfid='%s';"%(delete_bfid,)
                try:
                    res = db.query(d_q)
                    if len(res) != 0:
                        print('del result', res)
                except:
                    exc_type, exc_value = sys.exc_info()[:2]
                    print('delete exception: %s %s'%(str(exc_type), str(exc_value)))
                
        make_active = raw_input('make active bfid: ')
        if make_active:
            res = fcc.set_deleted('no', bfid = make_active)
            if not e_errors.is_ok(res):
                print('make active returned', res)
        
"""
if check_these:
    print('Check the following %s'%('check_multiple_list',))
    with open('check_multiple_list', 'w') as f:
        f.write('%s\n'%(check_these,))
"""
db.close()
