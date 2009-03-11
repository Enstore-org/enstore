#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

##Make the top level web page for an Instance of Enstore.

# system imports
import os
import string
import socket
import pg

# enstore modules
import enstore_html
import enstore_files
import enstore_constants
import HTMLgen
import web_server
import configuration_client
import Trace
import option

TITLE="ENSTORE SYSTEM INFORMATION"
LINKCOLOR="#0000EF" 
VLINKCOLOR="#55188A"
ALINKCOLOR="#FF0000"
BGCOLOR="#FFFFFF"
TEXTSIZE="+2"
TEXTCOLOR="#000066"
TITLESIZE="+5"
TITLECOLOR="#770000"
TABLECOLOR="#DFF0FF"
HTMLFILE="enstore_system.html"

def add_row_to_table(table,link,name,explanation):
    tr=HTMLgen.TR()
    tr.append(HTMLgen.TD(HTMLgen.Bold(HTMLgen.Font(HTMLgen.Href(link,name), size=TEXTSIZE,color=TEXTCOLOR),align="LEFT")))
    tr.append(HTMLgen.TD(HTMLgen.Font(explanation,size=TEXTSIZE,color=TEXTCOLOR),valign="CENTER"))
    table.append(tr)
        

class EnstoreSystemHtml:
    def __init__(self,name,bytes="0.0",remote=False):
        self.page=enstore_html.EnBaseHtmlDoc(refresh=60)
        self.page.title=TITLE
        self.page.linkcolor=LINKCOLOR
        self.page.vlinkcolor=VLINKCOLOR
        self.page.alinkcolor=ALINKCOLOR
        self.page.bgcolor=BGCOLOR


        global_table=HTMLgen.TableLite(cellpadding=0,cellspacing=0,border=0)
        global_table.append(HTMLgen.TR(enstore_html.empty_data()))
        global_table.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.Center(HTMLgen.EM(
            HTMLgen.Font(string.upper(name),
                         size=TITLESIZE, color=TITLECOLOR,align="center"))))))
        global_table.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.Center(HTMLgen.Image("ess.gif",align="CENTER")))))
        global_table.append(HTMLgen.TR(enstore_html.empty_data()))


        ###
        ### User Data Table
        ###
        self.user_data_table=HTMLgen.TableLite(cellpadding=2,cellspacing=2,border=4)
        t2tr1=HTMLgen.TR()
        t2tr1.append(HTMLgen.TD(HTMLgen.Bold(HTMLgen.Font("User Data on Tape &nbsp;:&nbsp;",
                                                          html_escape='OFF',
                                                          size="+2"))))
        t2tr1.append(HTMLgen.TD(HTMLgen.Font("%s TiB"%(bytes),
                                             html_escape='OFF',
                                             size="+2",color=TITLECOLOR),
                                bgcolor="#FFFFF0"))
        self.user_data_table.append(t2tr1)
        
        global_table.append(HTMLgen.TR(HTMLgen.TD(self.user_data_table,align="CENTER")))
        global_table.append(HTMLgen.TR(enstore_html.empty_data()))


        ###
        ### Status Table
        ###
        self.status_table=HTMLgen.TableLite(cellpadding=2,
                                            bgcolor=TABLECOLOR,
                                            cellspacing=5, border=2)
        self.status_table.width="100%"

        add_row_to_table(
            self.status_table,
            os.path.join(enstore_constants.OLD_WEB_SUBDIR,
                         enstore_constants.SAAGHTMLFILE), #enstore_saag.html
            "Enstore System Summary",
            "Enstore System Status-At-A-Glance")
        add_row_to_table(
            self.status_table,
            os.path.join(enstore_constants.OLD_WEB_SUBDIR,
                         enstore_files.status_html_file_name()), #status_enstore_system.html
            "Enstore Server Status",
            "Current status of the Enstore servers")
        add_row_to_table(
            self.status_table,
            os.path.join(enstore_constants.OLD_WEB_SUBDIR,
                         enstore_files.encp_html_file_name()), #encp_enstore_system.html
            "encp History",
            "History of recent encp requests")
        add_row_to_table(
            self.status_table,
            os.path.join(enstore_constants.OLD_WEB_SUBDIR,
                         enstore_files.config_html_file_name()), #config_enstore_system.html
            "Configuration",
            "Current Enstore System Configuration")
        add_row_to_table(self.status_table,
                         "enstore_alarms.html",  #enstore_constants.ALARM_HTML_FILE
                         "Alarms",
                         "Active alarms and alarm history")
        add_row_to_table(self.status_table,
                         "enstore_logs.html",   #enstore_constants.ALARM_HTML_FILE
                         "Log Files",
                         "Hardware and software log files")
        add_row_to_table(
            self.status_table,
            os.path.join(enstore_constants.TAPE_INVENTORY_SUBDIR,
                         "VOLUME_QUOTAS"),  #Need constant???
                         "Quota and Usage",
                         "How tapes are allocated and being used")
        add_row_to_table(self.status_table,
                         os.path.join(enstore_constants.PLOTS_SUBDIR,
                                      enstore_files.plot_html_file_name()),  #plot_enstore_system.html
                         "Plots",
                         "Enstore Plots")
        add_row_to_table(self.status_table,
                         "%s/%s" % (enstore_constants.WEB_SUBDIR,
                                    enstore_files.generated_web_page_html_file_name()), #generated_web_pages.html
                         "Web Pages",
                         "Enstore Web Pages")
        add_row_to_table(self.status_table,
                         "/cgi-bin/enstore_show_inv_summary_cgi.py", #Need constant???
                         "Tape Inventory Summary",
                         "Summary of inventory results")
        add_row_to_table(self.status_table,
                         "/cgi-bin/enstore_show_inventory_cgi.py", #Need constant???
                         "Tape Inventory",
                         "Detailed list of tapes and their contents")
        #add_row_to_table(self.status_table,
        #                 "enstore_quotas.html",
        #                 "Tape Quotas",
        #                 "Plots of tape quotas")
        add_row_to_table(self.status_table,
                         "cron_pics.html",  #Need constant???
                         "Cronjob Status",
                         "Lots of cronjob exit status for past week")
        
                                    
        global_table.append(HTMLgen.TR(self.status_table))
        global_table.append(HTMLgen.TR(enstore_html.empty_data()))

        global_table.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.Center(HTMLgen.Image("info.gif",align="CENTER")))))
        global_table.append(HTMLgen.TR(enstore_html.empty_data()))
        

        ###
        ### Info Table
        ###
        self.info_table=HTMLgen.TableLite(cellpadding=2,bgcolor=TABLECOLOR,cellspacing=5,border=2)
        self.info_table.width="100%"
        
        if not remote:
            add_row_to_table(self.info_table,"http://www-ccf.fnal.gov/enstore","Mass Storage System Main Page","Storage links for Enstore and dCache")
        add_row_to_table(self.info_table,"http://www-ccf.fnal.gov/enstore/documentation.html","Mass Storage System Documentation Page",
                         "Documentation, reports, talks for Enstore and dCache")
        if not remote:
            add_row_to_table(self.info_table,"http://www-isd.fnal.gov/enstore/enstore_status_only.html",
                                    "Production System's Overall Status","Status for all production Enstore systems")
            
        
        
        global_table.append(HTMLgen.TR(self.info_table))
        global_table.append(HTMLgen.TR(enstore_html.empty_data()))
        self.page.append(HTMLgen.Center(global_table))
        if not remote:
            self.page.append(HTMLgen.BR())
            self.page.append(HTMLgen.Href("http://www.fnal.gov/pub/disclaim.html","Legal Notices"))

    def write_html_page_to_directory(self,path=""):
        f=open(os.path.join(path,HTMLFILE),"w")
        f.write(str(self.page))
        f.close()




def do_work(intf):
    #rc=0
    server = web_server.WebServer()
    remote=True

    if socket.gethostbyname(socket.gethostname())[0:7] == "131.225" :
        print "We are in Fermilab", server.get_system_name()
        remote=False
    q="select coalesce(sum(size),0) from file, volume where file.volume = volume.id and system_inhibit_0 != 'DELETED' and media_type!='null'"
    
    if server.get_system_name().find("stken") != -1:
         q="select sum(deleted_bytes+unknown_bytes+active_bytes)  from volume where system_inhibit_0!='DELETED' and media_type!='null'"

    config_server_client_dict = configuration_client.get_config_dict()
    acc            = config_server_client_dict.get("database", {})
    
    byte_count = 0.0
    try: 
        db = pg.DB(host  = acc.get('db_host', "localhost"),
                   dbname= acc.get('dbname', "enstoredb"),
                   port  = acc.get('db_port', 5432),
                   user  = acc.get('dbuser', "enstore"))
        res=db.query(q)
        for row in res.getresult():
            if not row:
                continue
            byte_count = float(row[0])/(1024.*1024.*1024.*1024.)
        db.close()
    except:
        Trace.handle_error()
        pass

    name=server.get_system_name()
    if not name:
        name="unknown"
        
    main_web_page=EnstoreSystemHtml(name, "%8.2f" % (byte_count), remote)

    html_dir=None
    if server.inq_d.has_key("html_file"):
        html_dir=server.inq_d["html_file"]
    else:
        html_dir = enstore_files.default_dir
    if not os.path.exists(html_dir):
        os.makedirs(html_dir)
    main_web_page.write_html_page_to_directory(html_dir)

if __name__ == "__main__":

    intf_of_html = option.Interface()

    do_work(intf_of_html)
