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

import web_server

import os
import string

if __name__ == "__main__":
    rc=0
    server = web_server.WebServer()
    lines=[]
    f=open("enstore_system_top.html","r")
    try:
        for line in f:
            lines.append(line)
    except:
            rc=1
    f.close()

    lines.append(" <TR><TD><CENTER><I><FONT SIZE=+5 color=\"#770000\"> %s </FONT></I></CENTER></TD></TR>\n"%string.upper(server.get_system_name()))
    for fn in ["enstore_system_top-2.html","enstore_system_top-3.html"]:
        f=open(fn,"r")
        try:
            for line in f:
                lines.append(line)
        except:
            rc=1
        f.close()
    lines.append("0.00 TB\n")

    for fn in ["enstore_system_middle.html","enstore_system_info.html"]:
        f=open(fn,"r")
        try:
            for line in f:
                lines.append(line)
        except:
            rc=1
        f.close()
    html_dir=None
    if server.inq_d.has_key("html_file"):
        html_dir=server.inq_d["html_file"]
    else:
        html_dir = enstore_files.default_dir
    if not os.path.exists(html_dir):
        os.makedirs(html_dir)
    f=open(os.path.join(html_dir,"enstore_system.html"),"w");
    for line in lines:
        f.write(line)
    f.close()
    os.chmod(os.path.join(html_dir,"enstore_system.html"),0644)
    if os.path.exists( os.path.join(html_dir,"index.html")):
        os.symlink(os.path.join(html_dir,"enstore_system.html"), os.path.join(html_dir,"index.html"));
    os.system("cp *.gif %s"%html_dir);
    os.system("cp *.html %s"%html_dir);
    
    
           
