#!/usr/bin/env python
from __future__ import print_function
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


def print_error(volume):
    print("Content-Type: text/html")     # HTML is following
    print()                               # blank line, end of headers
    print("<html>")
    print("<head>")
    print("<title> File " + volume + "</title>")
    print("</head>")
    print("<body bgcolor=#ffffd0>")
    print("<font color=\"red\" size=10> No Such file " + volume + "</font>")
    print("</body>")
    print("</html>")


def print_header(txt):
    print("Content-Type: text/html")     # HTML is following
    print()                               # blank line, end of headers
    print("<html>")
    print("<head>")
    print("<title> " + txt + " </title>")
    print("</head>")
    print("<body bgcolor=#ffffd0>")


def print_footer():
    print("</body>")
    print("</html>")


def print_file_summary(ticket):
    print("<pre>")
    pprint.pprint(ticket)
    print("</pre>")


if __name__ == "__main__":
    form = cgi.FieldStorage()
    bfid = 'GCMS121122382500000'
    bfid = form.getvalue("bfid", "unknown")
    intf = info_client.InfoClientInterface(user_mode=0)
    intf.bfid = bfid
    ifc = info_client.infoClient(
        (intf.config_host,
         intf.config_port),
        None,
        intf.alive_rcv_timeout,
        intf.alive_retries)
    ticket = ifc.handle_generic_commands(enstore_constants.INFO_SERVER, intf)
    ticket = ifc.bfid_info(intf.bfid)
    if ticket['status'][0] == e_errors.OK:
        status = ticket['status']
        del ticket['status']
        ticket['status'] = status
    print_header(bfid)
    print('<h2><font color=#aa0000>', bfid, '</font></h2>')
    print_file_summary(ticket)
    print_footer()
