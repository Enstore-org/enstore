import os
import sys
import string
import signal
import time

import enstore_functions
import enstore_constants
import enstore_files

nodes = {'d0ensrv2' : ' enstore on d0en',
	 'stkensrv2' : ' enstore on stken',
	 'cdfensrv2' : ' enstore on cdfen',
	 }
DESTDIR = "/tmp/enstore_overall_status"
MYNAME = "EN_OVERALL_STAT"
LAST_STATUS_FILE = "last_status"
LAST_STATUS = "%s/%s.py"%(DESTDIR, LAST_STATUS_FILE)

DOWN_L = [enstore_constants.DOWN,
	  enstore_functions.format_time(time.time()),
	  enstore_constants.NONE,
	  enstore_constants.NONE,
	  enstore_constants.NONE,
	  enstore_constants.NONE]

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
	enstore_functions.send_mail(MYNAME, "%s not reachable to rcp overall status file"%(node,),
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
    thisNode = enstore_functions.strip_node(os.uname()[1])

    # fetch the files from the other nodes.  we will put them
    # in /tmp/enstore_status and import them from there
    # do some setup first
    setup_for_files()
    keys = nodes.keys()
    html_dir = enstore_functions.get_html_dir()
    file = "%s/%s"%(html_dir, enstore_constants.ENSTORESTATUSFILE)
    status_d = {}
    # get the last status of the enstore balls
    last_status_d = get_last_status()
    for node in keys:
	node = enstore_functions.strip_node(node)
        # make sure node is up before rcping
        if enstore_functions.ping(node) == enstore_constants.ALIVE:
            # this must match with the import below
            NEWFILE = "enstore_status_only_%s"%(node,)
            new_file = "%s/%s.py"%(DESTDIR, NEWFILE)
            if node == thisNode:
                rtn = os.system("cp %s %s"%(file, new_file))
            else:
                rtn = enstore_functions.get_remote_file(node, file, new_file)
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
    filename = "%s/%s"%(html_dir, enstore_constants.STATUSONLYHTMLFILE)
    only_file = enstore_files.HtmlStatusOnlyFile(filename)
    only_file.open()
    only_file.write(status_d, nodes)
    only_file.close()
    only_file.install()


if __name__ == "__main__" :

    do_work()
