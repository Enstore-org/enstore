import os
import sys
import string
import signal
import time

import enstore_functions2
import enstore_constants
import enstore_html
import HTMLgen

nodes = {'d0ensrv2' : ' d0en mass storage',
	 'stkensrv2' : ' stken mass storage',
	 'cdfensrv2' : ' cdfen mass storage',
	 }
TMP = ".tmp"
DESTDIR = "/tmp/enstore_overall_status"
MYNAME = "EN_OVERALL_STAT"
LAST_STATUS_FILE = "last_status"
LAST_STATUS = "%s/%s.py"%(DESTDIR, LAST_STATUS_FILE)
LCL_HTML_DIR = "/export/hppc_home/www/enstore/"

DOWN_L = [enstore_constants.DOWN,
	  enstore_functions2.format_time(time.time()),
	  enstore_constants.NONE,
	  enstore_constants.NONE,
	  enstore_constants.NONE,
	  enstore_constants.NONE]

class HtmlStatusOnlyFile:

    # we need to save both the file name passed to us and the one we will
    # write to.  we will create the temp one and then move it to the real
    # one.
    def __init__(self, name):
        self.file_name = name+TMP
        self.openfile = 0
	self.opened = 0
        self.real_file_name = name

    def open(self, mode='w'):
        try:
            self.openfile = open(self.file_name, mode)
	    self.opened = 1
        except IOError:
            self.openfile = 0
	    self.opened = 0

    def install(self):
	# move the file we created to the real file name
        if (not self.real_file_name == self.file_name) and os.path.exists(self.file_name):
	    os.system("mv %s %s"%(self.file_name, self.real_file_name))

    def close(self):
	if self.openfile:
	    self.openfile.close()
	    self.openfile = 0

    def do_write(self, data, filename=None):
	if filename is None:
	    filename = self.file_name
	try:
	    self.openfile.write(data)
	except IOError, detail:
	    print "Error writing %s (%s)"%(filename, detail)

    def write(self, status, nodes_d):
        if self.openfile:
            doc = enstore_html.EnStatusOnlyPage()
            doc.body(status, nodes_d)
	    self.do_write(str(doc))

def setup_for_files():
    if not os.path.isdir(DESTDIR):
	# make the directory
	os.system("mkdir %s\ntouch %s"%(DESTDIR, LAST_STATUS))
    else:
	# make the last status file if it does not exist
	if not os.path.exists(LAST_STATUS):
	    os.system("touch %s"%(LAST_STATUS,))
    # add this path to our python search directories
    sys.path.append(DESTDIR)
    os.environ['PYTHONPATH'] = string.join(sys.path, ':')

def mark_enstore_down(status_d, node, last_status_d):
    status_d[node] = DOWN_L
    # send mail about this, only send mail if we have been down continuously and last
    # mail was sent > 1 hour ago. so, 
    #
    #    if last_status was good, then send mail
    send_mail = 0
    ctr = last_status_d.get(node, 0)
    if ctr == 0:
	# either it was good last time, or it did not exist, in either case send mail
	send_mail = 1
    elif ctr == (ctr/20)*20:
	# only send mail every approx two hours the node is seen down continuously
	send_mail = 1
    if send_mail == 1:
	enstore_functions2.send_mail(MYNAME, "%s not reachable to rcp overall status file"%(node,),
				    "Overall status page has Enstore ball for %s as red"%(node,))
    
def get_last_status():
    last_s = {}
    exec("import %s\nlast_s = last_status.__dict__.get('status_d', {})\n"%(LAST_STATUS_FILE,))
    return last_s

def set_last_status(status_d):
    fd = open(LAST_STATUS, 'w')
    fd.write("status_d = %s"%(status_d,))
    fd.close()

def do_work():
    # where are we running, don't have to rcp to there
    thisNode = enstore_functions2.strip_node(os.uname()[1])

    # fetch the files from the other nodes.  we will put them
    # in /tmp/enstore_status and import them from there
    # do some setup first
    setup_for_files()
    keys = nodes.keys()
    # we hard code the html_dir because we are no longer running on an enstore system
    html_dir = "/local/ups/prd/www_pages/enstore/"
    file = "%s/%s"%(html_dir, enstore_constants.ENSTORESTATUSFILE)
    status_d = {}
    # get the last status of the enstore balls
    last_status_d = get_last_status()
    for node in keys:
	node = enstore_functions2.strip_node(node)
        # make sure node is up before rcping
        if enstore_functions2.ping(node) == enstore_constants.ALIVE:
            # this must match with the import below
            NEWFILE = "enstore_status_only_%s"%(node,)
            new_file = "%s/%s.py"%(DESTDIR, NEWFILE)
            if node == thisNode:
                rtn = os.system("cp %s %s"%(file, new_file))
            else:
                rtn = enstore_functions2.get_remote_file(node, file, new_file)
            if rtn == 0:
                exec("import %s\nstatus_d[node] = %s.status\n"%(NEWFILE, NEWFILE))
            else:
                # there was an error, mark enstore as down
                mark_enstore_down(status_d, node, last_status_d)
        else:
            # there was an error, mark enstore as down
            mark_enstore_down(status_d, node, last_status_d)
	if status_d[node][0] == enstore_constants.DOWN:
	    last_status_d[node] = last_status_d.get(node, 0) + 1
	else:
	    last_status_d[node] = 0
    else:
	set_last_status(last_status_d)
            
    # now create the web page
    filename = "%s/%s"%(LCL_HTML_DIR, enstore_constants.STATUSONLYHTMLFILE)
    only_file = HtmlStatusOnlyFile(filename)
    only_file.open()
    only_file.write(status_d, nodes)
    only_file.close()
    only_file.install()


if __name__ == "__main__" :

    do_work()
