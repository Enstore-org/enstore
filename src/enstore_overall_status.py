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

def setup_for_files():
    if not os.path.isdir(DESTDIR):
	# make the directory
	os.system("mkdir %s"%(DESTDIR,))
    # this path to our python search directories
    sys.path.append(DESTDIR)
    os.environ['PYTHONPATH'] = string.join(sys.path, ':')

def get_remote_file(node, file, newfile):
    # we have to make sure that the rcp does not hang in case the remote node is goofy
    pid = os.fork()
    if pid == 0:
	# this is the child
	rtn = os.system("enrcp %s:%s %s/%s.py"%(node, file, DESTDIR, newfile))
	os._exit(rtn)
    else:
	# this is the parent, allow a total of 30 seconds for the child
	for i in [0, 1, 2, 3, 4, 5]:
	    rtn = os.waitpid(pid, os.WNOHANG)
	    if rtn[0] == pid:
		return rtn[1] >> 8   # pick out the top 8 bits as the return code
	    time.sleep(5)
	else:
	    # the child has not finished, be brutal. it may be hung
	    os.kill(pid, signal.SIGKILL)
	    return 1

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
    for node in keys:
	node = enstore_functions.strip_node(node)
	# this must match with the import below
	NEWFILE = "enstore_status_only_%s"%(node,)
	if node == thisNode:
	    rtn = os.system("cp %s %s/%s.py"%(file, DESTDIR, NEWFILE))
	else:
	    rtn = get_remote_file(node, file, NEWFILE)
	if rtn == 0:
	    exec("import %s\nstatus_d[node] = %s.status\n"%(NEWFILE, NEWFILE))

    # now create the web page
    filename = "%s/%s"%(html_dir, enstore_constants.STATUSONLYHTMLFILE)
    only_file = enstore_files.HtmlStatusOnlyFile(filename)
    only_file.open()
    only_file.write(status_d, nodes)
    only_file.close()
    only_file.install()


if __name__ == "__main__" :

    do_work()
