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
HTTPD_CONF="/etc/httpd/conf/httpd.conf"
SUFFIX=".orig"
SERVER_ROOT="ServerRoot"

import errno
import configuration_client
import enstore_constants
import sys
import types
import os
import e_errors
import getopt
import string
import socket

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
        else:
            try: 
                configfile = os.environ.get('ENSTORE_CONFIG_FILE')
                print "Failed to connect to config server, using configuration file %s"%(configfile,)
                f = open(configfile,'r')
                code = string.join(f.readlines(),'')
                configdict={}
                exec(code)
                self.config_dict=configdict
                self.server_dict = self.config_dict.get(WEB_SERVER, {})
                ret =configdict['known_config_servers']
 		def_addr = (os.environ['ENSTORE_CONFIG_HOST'],
			    int(os.environ['ENSTORE_CONFIG_PORT']))
                for item in ret.items():
                    if socket.getfqdn(item[1][0]) == socket.getfqdn(def_addr[0]):
                        self.system_name = item[0]
                self.web_server = self.config_dict.get(WEB_SERVER, {})
               
            except:
                print "Config file ",configfile," does not exist"
                self.is_ok=False
                sys.exit(1)
        self.inq_d = self.config_dict.get(enstore_constants.INQUISITOR, {})
        self.Root = self.server_dict.get(SERVER_ROOT,'/etc/httpd')
        self.config_file = "%s/conf/httpd.conf"%(self.Root)

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

    def move_httpd_conf(self,src,dest):
        if os.access(src, os.F_OK):
            try: 
                os.rename(src,dest)
            except:
                print "Failed to move original httpd.conf file"
                return 1
        return 0
    
    def write_httpd_conf(self) :
        rc=0
        if not self.is_ok :
            return 1
        if self.move_httpd_conf(self.config_file,"%s%s"%(self.config_file,SUFFIX)) :
            return 1
        f=open(self.config_file,"w")
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
        if self.server_dict.has_key('DocumentRoot'):
            return self.server_dict['DocumentRoot']
        else:
            print 'DocumentRoot is not defined in the configuration'
            return None

    def get_pid_file(self):
        if not self.is_ok :
            return None
        if self.server_dict.has_key('PidFile'):
            return self.server_dict['PidFile']
        else:
            print 'PidFile is not defined in the configuration'
            return None

    def get_server_name(self):
        if not self.is_ok :
            return None
        if self.server_dict.has_key('ServerName'):
            return self.server_dict['ServerName']
        else:
            print 'ServerName is not defined in the configuration'
            return None

    def get_server_root(self):
        if not self.is_ok :
            return None
        if self.server_dict.has_key('ServerRoot'):
            return self.server_dict['ServerRoot']
        else:
            print 'ServerRoot is not defined in the configuration'
            return None

    def get_error_log(self):
        if not self.is_ok :
            return None
        if self.server_dict.has_key('ErrorLog'):
            return self.server_dict['ErrorLog']
        else:
            print 'ErrorLog is not defined in the configuration'
            return None

    def get_custom_log(self,name):
        if not self.is_ok :
            return None
        if not self.is_ok :
            return None
        if self.server_dict.has_key('CustomLog'):
            return self.server_dict['CustomLog'][name]
        else:
            print 'CustomLog is not defined in the configuration'
            return None

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
            print "failed to open ",file_name
            rc=1
        return rc

def install():
    server = WebServer()
    rc=0
    try:
        if server.read_httpd_conf():
            print "Failed to read httpd.conf"
            return 1
        if server.write_httpd_conf():
            print "Failed to write httpd.conf"
            return 1
        doc_root = server.get_document_root()
        if doc_root:
            if not os.path.exists(doc_root):
                os.makedirs(doc_root)
        else:
            return 1
        pid_file = server.get_pid_file()
        if pid_file:
            if not os.path.exists(os.path.dirname(pid_file)):
                os.makedirs(os.path.dirname(pid_file))
        else:
            return 1
        error_log = server.get_error_log()
        if error_log:
            if not os.path.exists(os.path.dirname(error_log)):
                os.makedirs(os.path.dirname(error_log))
        else:
            return 1
        combined = server.get_custom_log('combined')
        if combined:
            if not os.path.exists(os.path.dirname(combined)):
                os.makedirs(os.path.dirname(combined))
        else:
            return 1
        referer = server.get_custom_log('referer')
        if referer:
            if not os.path.exists(os.path.dirname(referer)):
                os.makedirs(os.path.dirname(referer))
        else:
            return 1
        agent = server.get_custom_log('agent')
        if agent:
            if not os.path.exists(os.path.dirname(agent)):
                os.makedirs(os.path.dirname(agent))
        else:
            return 1
        if server.generate_top_index_html():
            print "Failed to create index.html"
            return 1
        html_dir=None
        if server.inq_d.has_key("html_file"):
            html_dir=server.inq_d["html_file"]
        else:
            html_dir = enstore_files.default_dir
        print "exists",os.path.exists(os.path.join(server.get_document_root(),"enstore")),os.path.join(server.get_document_root(),"enstore")
        if not os.path.exists(html_dir):
            os.makedirs(html_dir)
        if not os.path.exists(os.path.join(server.get_document_root(),"enstore")):
            os.symlink(html_dir,os.path.join(server.get_document_root(),"enstore"))

    except (KeyboardInterrupt, IOError, OSError):
        exc, msg, tb = sys.exc_info()
        import traceback
        for l in traceback.format_exception( exc, msg, tb ):
            print l
        return 1
    #except:
    #    return 1
    return 0

def erase():
    server = WebServer()
    rc=0
    move_httpd_conf(server.config_file,"%s%s"%(server.config_file,SUFFIX))
        
def usage(cmd):
    print "Usage: %s -i [--install] -e [erase] -h [--help]"%(cmd,)

if __name__ == "__main__":
    try:
        do_erase=False
        do_install=False
        opts, args = getopt.getopt(sys.argv[1:], "hs:is:es:", ["help","install","erase"])
        for o, a in opts:
            if o in ("-h", "--help"):
                usage(sys.argv[0])
                sys.exit(1)
            if o in ("-i", "--install"):
                do_install=True
            if o in ("-e", "--erase"):
                do_erase=True
        if do_erase and do_install :
            print "One switch at a time is supported \"-i\" or \"-e\" "
            sys.exit(1)
        if do_erase :
            if erase() :
                print "Failed to erase directories"
                sys.exit(1)
        if do_install :
            if install() :
                print "Failed to install directories"
                sys.exit(1)
        
    except getopt.GetoptError:
        print "Failed to process arguments"
        usage(sys.argv[0])
        sys.exit(2)

        
        

                                    
