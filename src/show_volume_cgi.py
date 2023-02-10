#!/usr/bin/env python
import cgi
import os
import string
import time
import sys
import pprint

import enstore_utils_cgi
import enstore_constants
import info_client
import e_errors

kk = 1024
mm = kk * kk
gg = mm * kk

def print_error(volume):
    print "Content-Type: text/html"     # HTML is following
    print                               # blank line, end of headers
    print "<html>"
    print "<head>"
    print "<title> Volume "+volume+"</title>"
    print "</head>"
    print "<body bgcolor=#ffffd0>"
    print "<font color=\"red\" size=10> No Such volume "+ volume + "</font>"
    print "</body>"
    print "</html>"

def print_header(txt):
    print "Content-Type: text/html"     # HTML is following
    print                               # blank line, end of headers
    print "<html>"
    print "<head>"
    print "<title> " + txt +" </title>"
    print "</head>"
    print "<body bgcolor=#ffffd0>"

def print_footer():
    print "</body>"
    print "</html>"


def show_size(s):
    if s > gg:
        return "%7.2f GB"%(float(s) / gg)
    elif s > mm:
        return "%7.2f MB"%(float(s) / mm)
    elif s > kk:
        return "%7.2f KB"%(float(s) / kk)
    else:
        return "%7d Bytes"%(s)

def print_volume_summary(ticket):
    la_time='(unknown)'
    if ticket['last_access'] :
        if int(ticket['last_access'].split(' ')[-1])<1970:
            la_time='(never)'
        else:
            la_time=ticket['last_access']
    print "<font size=5 color=#0000aa><b>"
    print "<pre>"
    print "          Volume:", ticket['external_label']
    print "Last accessed on:", la_time
    print "      Bytes free:", show_size(ticket['remaining_bytes'])
    print "   Bytes written:", show_size(ticket.get('active_bytes',0L)+ticket.get('deleted_bytes',0L))
    print "        Inhibits:", ticket['system_inhibit'][0],"+",ticket['system_inhibit'][1]
    print '</b><hr></pre>'
    print "</font>"
    print "<pre>"
    pprint.pprint(ticket)
    print "<hr></pre>"


def print_volume_content(ticket,list):
    format = "%%-%ds <a href=/cgi-bin/show_file_cgi.py?bfid=%%s>%%-19s</a> %%10s %%-22s %%-7s <a href=/cgi-bin/show_file_cgi.py?bfid=%%s>%%-19s</a> %%-20s %%-20s %%s"%(len(list))
    header = " volume         bfid             size      location cookie     status     package_id      archive_status        cache_status            original path"
    print '<pre>'
    print '<font color=#aa0000>'+header+'</font>'
    print '<p>'
    tape=ticket['tape_list']
    for record in tape:
        color='#ff0000'
        deleted='unlnown'
        if record['deleted'] == 'yes':
            deleted = 'deleted'
        elif record['deleted'] == 'no':
            deleted = 'active'
            color='#0000ff'
        else:
            deleted = 'unknown'
        print '<font color=\"'+color+'\">', format % (intf.list,
                                                      record['bfid'],
                                                      record['bfid'],
                                                      record['size'],
                                                      record['location_cookie'], deleted,
                                                      record.get('package_id',None),
                                                      record.get('package_id',None),
                                                      record.get('archive_status',None),
                                                      record.get('cache_status',None),
                                                      record['pnfs_name0']
                                                      ), "</font>"
    print '</pre>'


if __name__ == "__main__":   # pragma: no cover
    form   = cgi.FieldStorage()
    volume = form.getvalue("volume", "unknown")
    intf   =   info_client.InfoClientInterface(user_mode=0)
    intf.gvol = volume
    intf.list = volume
    ifc    = info_client.infoClient((intf.config_host, intf.config_port), None, intf.alive_rcv_timeout, intf.alive_retries)
    ticket = ifc.handle_generic_commands(enstore_constants.INFO_SERVER ,intf)
    ticket = ifc.inquire_vol(intf.gvol)
    if ticket['status'][0] == e_errors.OK:
        status = ticket['status']
        del ticket['status']
        del ticket['non_del_files']
        ticket['declared'] = time.ctime(ticket['declared'])
        ticket['first_access'] = time.ctime(ticket['first_access'])
        ticket['last_access'] = time.ctime(ticket['last_access'])
        if ticket.has_key('si_time'):
            ticket['si_time'] = (time.ctime(ticket['si_time'][0]),
                                 time.ctime(ticket['si_time'][1]))
        ticket['status'] = status
    else:
        print_error(volume)
        sys.exit(1)
    print_header ("Volume %s"%(volume),)
    print '<h1><font color=#aa0000>', volume, '</font></h1>'
    print_volume_summary(ticket)
    f_ticket = ifc.tape_list(intf.list)
    if f_ticket['status'][0] == e_errors.OK:
        print_volume_content(f_ticket,intf.list)

    print_footer()
