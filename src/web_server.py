#!/usr/bin/env python
###############################################################################
#
# $Author$
# $Date$
# $Id$
#
# Apache Configurator
# Author: Dmitry Litvintsev (litvinse@fnal.gov) 06/08/07
#
###############################################################################

WEB_SERVER = "web_server"

import errno
import configuration_client
import enstore_constants
import sys
import types
import os
import e_errors

class WebServer:
    def __init__(self,timeout=1,retry=0):
        self.is_ok=True
        intf  = configuration_client.ConfigurationClientInterface(user_mode=0)
        csc   = configuration_client.ConfigurationClient((intf.config_host, intf.config_port))
        self.system_name = csc.get_enstore_system(timeout,retry)
        self.server_dict={}
        if self.system_name:
            self.server_dict = csc.get(WEB_SERVER, timeout, retry)
            config_dict = csc.dump(timeout, retry)
            self.config_dict = config_dict['dump']
            self.inq_d = self.config_dict.get(enstore_constants.INQUISITOR, {})
        else:
            print "Failed to connect to config server"
            self.is_ok=False

    def get_ok(self):
        return self.is_ok

    def get_system_name(self) :
        return self.system_name

    def set_ok(self,ok=True):
        self.is_ok=ok

    def set_system_name(self,name=None) :
        self.system_name=name

    def read_httpd_conf(self) :
        rc = 0
        if not self.is_ok :
            return 1
        self.lines=[]
        f=open("httpd.conf","r")
        try:
            for line in f:
                self.lines.append(line)
        except:
            rc=1
        f.close()
        return rc

    def write_httpd_conf(self) :
        rc=0
        if not self.is_ok :
            return 1
        print self.server_dict.keys()
        f=open("/etc/httpd/conf/httpd.conf","w")
        try: 
            for line in self.lines:
                txt = line
                if line.lstrip().find('#') != 0:
                    for key in self.server_dict.keys():
                        if key == "status" :
                            continue
                        indx=line.lstrip().find(key)
                        if indx == 0 :
                            if key == "CustomLog" :
                                #
                                # type of log files
                                # 
                                for k in self.server_dict[key].keys():
                                    index=line.strip().find(k)
                                    if index!=-1:
                                        txt = key + " " + str(self.server_dict[key][k])+" "+k+"\n"
                                        break
                            elif key == "ScriptAlias":
                                for k in self.server_dict[key].keys():
                                    txt = key + " " + str(self.server_dict[key]['fake'])+" "+ str(self.server_dict[key]['real']) +"\n"
                                    break
                            else:
                                txt = key + " " + str(self.server_dict[key]) +"\n"
                                break
                f.write(txt)
        except:
            rc=1
        f.close()
        return rc

    def get_document_root(self):
        if not self.is_ok :
            return None
        return self.server_dict['DocumentRoot']

    def get_pid_file(self):
        if not self.is_ok :
            return None
        return self.server_dict['PidFile']

    def get_server_name(self):
        if not self.is_ok :
            return None
        return self.server_dict['ServerName']

    def get_server_root(self):
        if not self.is_ok :
            return None
        return self.server_dict['ServerRoot']

    def get_error_log(self):
        if not self.is_ok :
            return None
        return self.server_dict['ErrorLog']

    def get_custom_log(self,name):
        if not self.is_ok :
            return None
        return self.server_dict['CustomLog'][name]

    def generate_top_index_html(self):
        rc=0
        if not self.is_ok :
            return 1
        dir_name=self.get_document_root()
        file_name=os.path.join(dir_name,"index.html")
        try:
            f=open(file_name,"w")
            body="<html>\n"+\
                  "<head>\n"+\
                  "<title>%s Homepage </title>\n"%self.get_server_name()+\
                  "</head>\n"+\
                  "<body>\n"+\
                  "<h1>%s Homepage</h1>\n"%self.get_server_name()+\
                  "</body>\n"+\
                  "</html>\n"
            f.write(body)
            f.close()
        except:
            rc=1
        return rc


if __name__ == "__main__":
    server = WebServer()
    try: 
        if server.read_httpd_conf():
            print "Failed to read httpd.conf"
            sys.exit(1)
        if server.write_httpd_conf():
            print "Failed to read httpd.conf"
            sys.exit(1)
        if not os.path.exists(os.path.dirname(server.get_document_root())):
            os.makedirs(os.path.dirname(server.get_document_root()))
        if not os.path.exists(os.path.dirname(server.get_pid_file())):
            os.makedirs(os.path.dirname(server.get_pid_file()))
        if not os.path.exists(os.path.dirname(server.get_error_log())):
            os.makedirs(os.path.dirname(server.get_error_log()))
        if not os.path.exists(os.path.dirname(server.get_custom_log('combined'))):
            os.makedirs(os.path.dirname(server.get_custom_log('combined')))
        if not os.path.exists(os.path.dirname(server.get_custom_log('referer'))):
            os.makedirs(os.path.dirname(server.get_custom_log('referer')))
        if not os.path.exists(os.path.dirname(server.get_custom_log('agent'))):
            os.makedirs(os.path.dirname(server.get_custom_log('agent')))
        if server.generate_top_index_html():
            print "Failed to create index.html"
            sys.exit(1)
        #
        # directory wehere all the stuff goes (usually /diska/www_pages) is
        # linked to  DocumentRoot/enstore
        #
        html_dir=None
        if server.inq_d.has_key("html_file"):
            html_dir=server.inq_d["html_file"]
        else:
            html_dir = enstore_files.default_dir
        os.symlink(html_dir,os.path.join(server.get_document_root(),"enstore"))

    except (KeyboardInterrupt, IOError, OSError):
        sys.exit(1)
        
        

                                    
