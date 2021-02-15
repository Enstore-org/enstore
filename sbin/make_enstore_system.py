#!/usr/bin/env python
###############################################################################
#
# $Author$
# $Date$
# $Id$
#
# Builds Enstore Main page
# Author: Dmitry Litvintsev (litvinse@fnal.gov) 06/08/07
#
###############################################################################

from __future__ import print_function
import web_server
import enstore_system_html
import socket
import enstore_files


import sys
import os
import string


def rchown(dir, uid, gid):
    try:
        os.chown(dir, uid, gid)
        if (os.path.isdir(dir)):
            direntries = os.listdir(os.path.abspath(dir))
            for direntry in direntries:
                d = os.path.abspath(os.path.join(dir, direntry))
                rchown(d, uid, gid)
    except (IOError, os.error) as why:
        print("rchown %s: %s" % (str(dir), str(why)))


def rchmod(dir, mode):
    try:
        os.chmod(dir, mode)
        if (os.path.isdir(dir)):
            direntries = os.listdir(os.path.abspath(dir))
            for direntry in direntries:
                d = os.path.abspath(os.path.join(dir, direntry))
                rchmod(d, mode)
    except (IOError, os.error) as why:
        print("rchmod %s: %s" % (str(dir), str(why)))


if __name__ == "__main__":
    rc = 0
    server = web_server.WebServer()
    hostname = socket.gethostname()
    if hostname != server.get_server_host():
        print("Doing nothing - we are not supposed to do anything here ")
        sys.exit(2)

    remote = True
    if socket.gethostbyname(socket.gethostname())[0:7] == "131.225":
        print("We are in Fermilab", server.get_system_name())
        remote = False

    main_web_page = enstore_system_html.EnstoreSystemHtml(server.get_system_name(),
                                                          {"total": 0, "active": 0}, remote)

    html_dir = None
    if "html_file" in server.inq_d:
        html_dir = server.inq_d["html_file"]
    else:
        html_dir = enstore_files.default_dir
    if not os.path.exists(html_dir):
        os.makedirs(html_dir)
    main_web_page.write_html_page_to_directory(html_dir)
    os.chmod(os.path.join(html_dir, "enstore_system.html"), 0o644)
    if not os.path.exists(os.path.join(html_dir, "index.html")):
        os.symlink(
            os.path.join(
                html_dir, enstore_system_html.HTMLFILE), os.path.join(
                html_dir, "index.html"))
    os.system("cp *.gif %s" % html_dir)
    os.system("cp *.html %s" % html_dir)
    uid = server.get_server_getpwuid()[2]
    gid = server.get_server_getpwuid()[3]
    try:
        rchown(html_dir, uid, gid)
    except BaseException:
        print("Failed to change ownership of ", html_dir, " to ", uid, gid)
        pass
    cgi_dir = server.get_cgi_directory()
    os.system("cp *cgi*py %s" % cgi_dir)
    os.system("cp active_volumes.sh %s" % cgi_dir)
    os.system("cp enstore_log_file_search_cgi.py %s/log" % cgi_dir)
    os.system("cp enstore_utils_cgi.py %s/log" % cgi_dir)
    try:
        rchown(cgi_dir, uid, gid)
    except BaseException:
        print("Failed to change ownership of ", cgi_dir, " to ", uid, gid)
        pass
    try:
        rchmod(cgi_dir, 0o755)
    except BaseException:
        print("Failed to permission  mask of ", cgi_dir, " to ", 0o755)
        pass
