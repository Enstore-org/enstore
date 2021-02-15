#!/usr/bin/env python
# $Id$
from __future__ import print_function
import os
import sys
import popen2
import configuration_client
import pprint


def get_movers(config):
    movers = {}
    for key in config.keys():
        if key.find('.mover') > 0 and key.find('null') == -1:
            movers[key] = config[key]
    return movers


def get_config(host, port):
    dict = configuration_client.get_config_dict()
    return dict


def get_db_refs(config_dict):
    ds = config_dict['drivestat_server']
    return ds['dbhost'], ds['dbport'], ds['dbname']


def match_mover_log_name(logname, movers):
    for mover in movers.keys():
        if logname == movers[mover]['logname']:
            rc = (mover, movers[mover])
            break
    else:
        rc = None
    return rc


def publish_results(report, out, config_host, system, server, web_s):
    import HTMLgen
    import enstore_html
    import enstore_functions2
    import time

    of = open(out, 'w')
    html_doc = HTMLgen.SimpleDocument(
        title="ENSTORE Tape Drives Information for %s" %
        (system,), background="enstore.gif", textcolor=enstore_html.DARKBLUE)
    html_doc.append(
        HTMLgen.Heading(
            2, "ENSTORE Tape Drives Information for %s" %
            (system,)))

    td = HTMLgen.TD(HTMLgen.Emphasis(HTMLgen.Font("Last updated : ",
                                                  size=-1)),
                    align="RIGHT")
    td.append(HTMLgen.Font("%s" % (enstore_functions2.format_time(time.time()),),
                           html_escape='OFF', size=-1))
    html_doc.append(td)

    tr = HTMLgen.TR(valign="CENTER")
    headings = ["Name", "Log. Name", "Host", "Serial #", "Type", "Firmware"]
    num_headings = len(headings)
    for hding in headings:
        tr.append(HTMLgen.TH(HTMLgen.Bold(HTMLgen.Font(hding, size="+2",
                                                       color=enstore_html.BRICKRED)),
                             align="CENTER"))
    en_table = HTMLgen.TableLite(tr, border=1, bgcolor=enstore_html.AQUA, width="100%",
                                 cols=num_headings, cellspacing=5,
                                 cellpadding=enstore_html.CELLP)

    keys = sorted(report.keys())
    for mv in keys:
        tr = HTMLgen.TR(HTMLgen.TD(mv))
        tr.append(HTMLgen.TD(report[mv]['stats']['logname']))
        tr.append(HTMLgen.TD(report[mv]['stats']['host']))
        tr.append(HTMLgen.TD(report[mv]['stats']['sn']))
        tr.append(HTMLgen.TD(report[mv]['stats']['type']))
        tr.append(HTMLgen.TD(report[mv]['stats']['fmw']))

        en_table.append(tr)

    html_doc.append(en_table)

    # of.write(str(table))
    of.write(str(html_doc))
    of.close()

    c = '$ENSTORE_DIR/sbin/enrcp %s %s:%s' % (out, server, web_s)
    print(c)
    os.system(c)


config_host = os.getenv('ENSTORE_CONFIG_HOST')
config_port = os.getenv('ENSTORE_CONFIG_PORT')
config_dict = get_config(config_host, config_port)
db_host, db_port, db_name = get_db_refs(config_dict)
web = config_dict.get('inquisitor')['html_file']
web_server = config_dict.get('inquisitor')['host']

movers = get_movers(config_dict)


# query stubs
#cmd = 'psql -h %s -p %s -o "%s" -c "SELECT %s FROM status WHERE DATE(time) > CURRENT_DATE-INTERVAL %s1day%s GROUP BY %s" %s'
#what='drive_sn,drive_vendor,product_type,host, logical_drive_name, firmware_version'

cmd = 'psql -h %s -p %s -U enstore -o "%s" -c "select distinct on (logical_drive_name) logical_drive_name as drive, host, product_type as type, drive_vendor as vendor, drive_sn as sn, firmware_version as firmware from status order by logical_drive_name, time desc" drivestat'

query_file = '/tmp/firmware_stat_report_tmp.%s' % (config_host,)
os.system("rm -rf %s" % (query_file,))

query_cmd = cmd % (db_host, db_port, query_file)
print(query_cmd)
# sys.exit()
pipeObj = popen2.Popen3(query_cmd)
if pipeObj is None:
    sys.exit(1)
stat = pipeObj.wait()
result = pipeObj.fromchild.readlines()  # result has returned string

#out_file = '/tmp/firmware_stat_report.%s.html'%(config_host,)
out_file = '/tmp/firmware_stat_report.html'

# process results
mover_table = {}
f = open(query_file, 'r')
recs = []
f.readline()
f.readline()
while True:
    line = f.readline()
    if not line:
        break
    ll = line.split()
    if len(ll) != 11:  # to be paranoid
        continue
    d = {'logname': ll[0],
         'host': ll[2],
         'type': ll[4],
         'vendor': ll[6],
         'sn': ll[8],
         'fmw': ll[10]
         }
    recs.append(d)

report_dict = {}
for rec in recs:

    mov = match_mover_log_name(rec['logname'], movers)
    if mov:
        m = mov[0].split('.')[0]
        report_dict[m] = {'config': mov[1],
                          'stats': rec
                          }

if 'system' in config_dict:
    system = config_dict['system'].get('name', config_host)
else:
    system = config_host
publish_results(report_dict, out_file, config_host, system, web_server, web)
