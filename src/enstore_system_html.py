import enstore_html
import enstore_files
import HTMLgen
import web_server
import os
import string
import socket
import pg
import configuration_client

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

        self.user_data_table=HTMLgen.TableLite(cellpadding=2,cellspacing=2,border=4)
        t2tr1=HTMLgen.TR()
        t2tr1.append(HTMLgen.TD(HTMLgen.Bold(HTMLgen.Font("User Data on Tape &nbsp;:&nbsp;",
                                                          html_escape='OFF',
                                                          size="+2"))))
        t2tr1.append(HTMLgen.TD(HTMLgen.Font("%s TB"%(bytes),
                                             html_escape='OFF',
                                             size="+2",color=TITLECOLOR),
                                bgcolor="#FFFFF0"))
        self.user_data_table.append(t2tr1)
        
        global_table.append(HTMLgen.TR(HTMLgen.TD(self.user_data_table,align="CENTER")))
        global_table.append(HTMLgen.TR(enstore_html.empty_data()))


        self.status_table=HTMLgen.TableLite(cellpadding=2,bgcolor=TABLECOLOR,cellspacing=5,border=2)
        self.status_table.width="100%"

        add_row_to_table(self.status_table,"enstore_saag.html","Enstore System Summary","Enstore System Status-At-A-Glance")
        add_row_to_table(self.status_table,"status_enstore_system.html","Enstore Server Status","Current status of the Enstore servers")
        add_row_to_table(self.status_table,"encp_enstore_system.html","encp History","History of recent encp requests")
        add_row_to_table(self.status_table,"config_enstore_system.html","Configuration","Current Enstore System Configuration")
        add_row_to_table(self.status_table,"enstore_alarms.html","Alarms","Active alarms and alarm history")
        add_row_to_table(self.status_table,"enstore_logs.html","Log Files","Hardware and software log files")
        add_row_to_table(self.status_table,"tape_inventory/VOLUME_QUOTAS","Quota and Usage","How tapes are allocated and being used")
        add_row_to_table(self.status_table,"plot_enstore_system.html","Plots","Inquisitor Plots")
        
        if not remote:
            add_row_to_table(self.status_table,"http://www-isd.fnal.gov/enstore/enstore_status_only.html",
                                    "Production System's Overall Status","Status for all production Enstore systems")
            add_row_to_table(self.status_table,"https://ngopcli.fnal.gov/cgi-bin/web_gui/web_gui.fcgi?role=enstore-admin&host=ngopcli&port=3111&Submit=Continue","Ngop Monitoring","Ngop monitoring of all production Enstore systems")
            
                                    
        global_table.append(HTMLgen.TR(self.status_table))
        global_table.append(HTMLgen.TR(enstore_html.empty_data()))

        global_table.append(HTMLgen.TR(HTMLgen.TD(HTMLgen.Center(HTMLgen.Image("info.gif",align="CENTER")))))
        global_table.append(HTMLgen.TR(enstore_html.empty_data()))
        

        self.info_table=HTMLgen.TableLite(cellpadding=2,bgcolor=TABLECOLOR,cellspacing=5,border=2)
        self.info_table.width="100%"

        add_row_to_table(self.info_table,"/cgi-bin/enstore_show_inv_summary_cgi.py",
                         "Tape Inventory Summary","Summary of inventory results")

        add_row_to_table(self.info_table,"enstore_quotas.html","Tape Quotas","Plots of tape quotas")
        if not remote:
            add_row_to_table(self.info_table,"cron_pics.html","Cronjob Status","lots of cronjob exit status for past week")
            add_row_to_table(self.info_table,"http://www-ccf.fnal.gov/enstore","Mass Storage System Main Page","Storage links for Enstore and dCache")
        add_row_to_table(self.info_table,"http://www-ccf.fnal.gov/enstore/documentation.html","Mass Storage System Documentation Page",
                         "Documentation, reports, talks for Enstore and dCache")
        
        
        
        
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


if __name__ == "__main__":

    rc=0
    server = web_server.WebServer()
    remote=True
    q="select sum(size)/1024./1024./1024./1024. from file, volume where file.volume = volume.id and system_inhibit_0 != 'DELETED' "
    if socket.gethostbyname(socket.gethostname())[0:7] == "131.225" :
        print "We are in Fermilab", server.get_system_name()
        remote=False
        q=q+"and library in ("
        for l in "cdf", "CDF-9940B", "CDF-LTO3", "CDF-LTO4","mezsilo", "samlto", "samm2", "sammam", "D0-9940B", "samlto2", "shelf-samlto", "D0-LTO3", "D0-LTO4", "9940",  "CD-9940B", "CD-LTO3", "CD-LTO4":
            q=q+"'"+l+"',"
        q=q[0:-1]
        q=q+")";
    print q;

    intf  = configuration_client.ConfigurationClientInterface(user_mode=0)
    config_server_client  = configuration_client.ConfigurationClient((intf.config_host, intf.config_port))
    acc            = config_server_client.get("database", {})
    db_server_name = acc.get('db_host')
    db_name        = acc.get('dbname')
    db_port        = acc.get('db_port',5432)
    name           = db_server_name.split('.')[0]
    name=db_server_name.split('.')[0]
    bytes=0.

    try: 
        db = pg.DB(host=db_server_name, dbname=db_name, port=db_port);
        res=db.query(q)
        for row in res.getresult():
            if not row:
                continue
            bytes=row[0]
        db.close()
    except:
        pass

    main_web_page=EnstoreSystemHtml(server.get_system_name(),
                                    str(bytes), remote)

    html_dir=None
    if server.inq_d.has_key("html_file"):
        html_dir=server.inq_d["html_file"]
    else:
        html_dir = enstore_files.default_dir
    if not os.path.exists(html_dir):
        os.makedirs(html_dir)
    main_web_page.write_html_page_to_directory(html_dir)
