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
FNAL_DOMAIN="131.225"
import errno
import traceback
import sys
import types
import os
import getopt
import string
import socket
import pwd
import configuration_client
import enstore_constants
import enstore_functions2
import enstore_files
import e_errors
import Trace


class WebServer:
    def __init__(self,timeout=1,retry=0):
        self.is_ok=True
        csc   = configuration_client.ConfigurationClient((enstore_functions2.default_host(),
                                                          enstore_functions2.default_port()))
        self.this_host_name=socket.gethostname()
        hostinfo = socket.getaddrinfo(self.this_host_name, None)
        self.system_name = csc.get_enstore_system(timeout,retry)
        self.server_dict={}
        self.domain_name=hostinfo[0][4][0][0:7]
        if self.system_name:
            self.server_dict = csc.get(WEB_SERVER, timeout, retry)
            config_dict = csc.dump(timeout, retry)
            self.config_dict = config_dict['dump']
        else:
            try:
                configfile = os.environ.get('ENSTORE_CONFIG_FILE')
                print "Failed to connect to config server, using configuration file %s"%(configfile,)
                try:
                    f = open(configfile,'r')
                except:
                    exc,msg=sys.exc_info()[:2]
                    print exc,msg

                code = string.join(f.readlines(),'')
                configdict={}
                exec(code)
                self.config_dict=configdict
                self.server_dict = self.config_dict.get(WEB_SERVER, {})
                ret =configdict['known_config_servers']
 		def_addr = (os.environ['ENSTORE_CONFIG_HOST'],
			    int(os.environ['ENSTORE_CONFIG_PORT']))
                match = False
                for item in ret.items():
                    if socket.getfqdn(item[1][0]) == socket.getfqdn(def_addr[0]):
                        match = True
                        self.system_name = item[0]
                        break
                if not match:
                    self.system_name=self.this_host_name.split('.')[0]
                self.web_server = self.config_dict.get(WEB_SERVER, {})

            except:
                print "Config file ",configfile," does not exist"
                self.is_ok=False
                sys.exit(1)
        self.server_dict['ServerTokens']='Prod'
        self.server_dict['Timeout']=300
        self.server_dict['KeepAlive']='On'
        #self.server_dict['AllowOverride']='All'
        self.inq_d = self.config_dict.get(enstore_constants.INQUISITOR, {})
        self.Root = self.server_dict.get(SERVER_ROOT,'/etc/httpd')
        self.config_file = "%s/conf/httpd.conf"%(self.Root)
        self.custom_config_file = "%s/conf.d/enstore.conf"%(self.Root)

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
        if not os.path.exists("%s%s"%(self.config_file,SUFFIX)) :
            if self.move_httpd_conf(self.config_file,"%s%s"%(self.config_file,SUFFIX)) :
                return 1
        f=open("%s%s"%(self.config_file,SUFFIX),"r")
        self.lines=f.readlines()[:]
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
        print "Writing file ",self.config_file, " Root ",self.Root
        rc=0
        if not self.is_ok :
            return 1
        f=open(self.config_file,"w")
        conf_d_f = open(self.custom_config_file, 'w')

        txt = '<Directory /> \n'
        txt = txt + 'Options FollowSymLinks \n'
        txt = txt + 'AllowOverride none \n'
        txt = txt + 'Require all granted\n'
        txt = txt + '</Directory>\n'
        conf_d_f.write(txt)

        try:
            for line in self.lines:
                custom_txt = ''
                txt = line
                if line.lstrip().find('#') != 0:
                    for key in self.server_dict:
                        if key == "status" :
                            continue
                        indx=line.lstrip().find(key)
                        if indx==0 and key == line.split()[0].strip():
                            if key == "CustomLog" :
                                #
                                # type of log files
                                #
                                for k in self.server_dict[key].keys():
                                    index=line.strip().find(k)
                                    if index!=-1:
                                        custom_txt = key + " " + str(self.server_dict[key][k])+" "+k+"\n"
                                        break
                            elif key == "ScriptAlias":
                                for k in self.server_dict[key].keys():
                                    txt = key + " " + str(self.server_dict[key]['fake'])+" "+ str(self.server_dict[key]['real']) +"\n"
                                    txt = txt + "<Directory \""+str(self.server_dict[key]['real'])+"\">\n"
                                    txt = txt + "   AllowOverride None\n"
                                    txt = txt + "   Deny from all\n"
                                    txt = txt + "   Order deny,allow\n"
                                    txt = txt + "   Allow from 127.0.0.1\n"
                                    if self.domain_name==FNAL_DOMAIN :
                                        txt = txt + "   Allow from  131.225\n"
                                        txt = txt + "   Allow from  137.138\n"
                                        txt = txt + "   Allow from  131.169\n"
                                        txt = txt + "   Allow from  131.225.73\n"
                                        txt = txt + "   Allow from  131.225.74\n"
                                    else:
                                        txt = txt + "   Allow from "+self.domain_name+"\n"
                                    for e in ["PYTHONINC", "PYTHONPATH", "PYTHONLIB", "PATH"]:
                                        txt = txt + "SetEnv "+e+" "+os.getenv(e)+"\n"
                                    txt = txt + "</Directory>\n"
                                    break
                            elif key == "ServerHost":
                                custom_txt = key + " " + self.this_host_name +"\n";
                            else:
                                custom_txt = key + " " + str(self.server_dict[key]) +"\n"
                                break
                f.write(txt)
                if custom_txt:
                    conf_d_f.write(custom_txt)
        except Exception, msg:
            print str(msg)
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


    def get_server_user(self):
        if not self.is_ok :
            return None
        if self.server_dict.has_key('User'):
            return self.server_dict['User']
        else:
            print 'DocumentRoot is not defined in the configuration'
            return None

    def get_server_getpwuid(self):
        user=self.get_server_user()
        if user :
            return pwd.getpwnam(user)
        else:
            return None

    def get_server_group(self):
        if not self.is_ok :
            return None
        if self.server_dict.has_key('Group'):
            return self.server_dict['Group']
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


    def get_server_host(self):
        if not self.is_ok :
            return None
        if self.server_dict.has_key('ServerHost'):
            return self.server_dict['ServerHost']
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
        if self.server_dict.has_key('CustomLog'):
            return self.server_dict['CustomLog'][name]
        else:
            print 'CustomLog is not defined in the configuration'
            return None

    def get_cgi_directory(self):
        if not self.is_ok :
            return None
        if self.server_dict.has_key('ScriptAlias'):
            return self.server_dict['ScriptAlias']['real']
        else:
            print 'ScriptAlias is not defined in the configuration'
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
    if not server.get_server_user() :
        print "Failed to determine user name under which web server is running "
        return 1
    try:
        if server.read_httpd_conf():
            print "Failed to read httpd.conf"
            return 1
        if server.write_httpd_conf():
            print "Failed to write httpd.conf"
            return 1
        if server.this_host_name != server.get_server_host():
            print "You are installing RPM on host which is not intended to run apache server "
            return 0
        doc_root = server.get_document_root()
        if doc_root:
            if not os.path.exists(doc_root):
                os.makedirs(doc_root)
        else:
            return 1
        cgi_dir = server.get_cgi_directory()
        if cgi_dir:
            if not os.path.exists(cgi_dir):
                os.makedirs(cgi_dir)
        else:
            return 1

        cgi_dir=cgi_dir+"/log"
        if not os.path.exists(cgi_dir):
            os.makedirs(cgi_dir)

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
        log_dir=server.config_dict.get('log_server', {})['log_file_path']
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        print "log directory ",log_dir
        log_dir_link=os.path.join(html_dir,"log")
        if not os.path.exists(log_dir_link):
            os.symlink(log_dir,log_dir_link)

    except (KeyboardInterrupt, IOError, OSError):
        exc, msg, tb = sys.exc_info()
        Trace.handle_error(exc, msg, tb)
        return 1
    #except:
    #    return 1
    return 0

def erase():
    server = WebServer()
    rc=0
    server.move_httpd_conf("%s%s"%(server.config_file,SUFFIX), server.config_file)

def usage(cmd):
    print "Usage: %s -i [--install] -e [erase] -h [--help]"%(cmd,)

if __name__ == "__main__":   # pragma: no cover
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





