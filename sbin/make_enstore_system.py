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
import enstore_system_html
import socket
import enstore_files


import sys
import os
import string

if __name__ == "__main__":
    rc=0
    server = web_server.WebServer()
    hostname = socket.gethostname()
    if hostname != server.get_server_host() :
        print "Doing nothing - we are not supposed to do anything here "
        sys.exit(2)
        
    remote=True
    if socket.gethostbyname(socket.gethostname())[0:7] == "131.225" :
        print "We are in Fermilab", server.get_system_name()
        remote=False

    main_web_page=enstore_system_html.EnstoreSystemHtml(server.get_system_name(),
                                                        "0.00", remote)

    html_dir=None
    if server.inq_d.has_key("html_file"):
        html_dir=server.inq_d["html_file"]
    else:
        html_dir = enstore_files.default_dir
    if not os.path.exists(html_dir):
        os.makedirs(html_dir)
    main_web_page.write_html_page_to_directory(html_dir)
    os.chmod(os.path.join(html_dir,"enstore_system.html"),0644)
    if not os.path.exists( os.path.join(html_dir,"index.html")):
        os.symlink(os.path.join(html_dir,main_web_page.HTMLFILE), os.path.join(html_dir,"index.html"));
    os.system("cp *.gif %s"%html_dir);
    os.system("cp *.html %s"%html_dir);
    uid=server.get_server_getpwuid()[2]
    try:
        os.chown(html_dir,server.get_server_getpwuid()[2],server.get_server_getpwuid()[3])
    except:
        print "Failed to change ownership of ",html_dir," to ",server.get_server_getpwuid()[2],server.get_server_getpwuid()[3]
        pass
    
    
    
             
    
    
           
