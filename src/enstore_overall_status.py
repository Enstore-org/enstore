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

def setup_for_files():
    if not os.path.isdir(DESTDIR):
	# make the directory
	os.system("mkdir %s"%(DESTDIR,))
    # this path to our python search directories
    sys.path.append(DESTDIR)
    os.environ['PYTHONPATH'] = string.join(sys.path, ':')

def mark_enstore_down(status_d, node):
    status_d[node] = [enstore_constants.DOWN,
                      enstore_functions.format_time(time.time()),
                      enstore_constants.NONE,
                      enstore_constants.NONE,
                      enstore_constants.NONE,
                      enstore_constants.NONE]
    # send mail about this
    enstore_functions.send_mail(MYNAME, "%s not reachable to rcp overall status file"%(node,),
				"Overall status page has Enstore ball for %s as red"%(node,))
    
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
                mark_enstore_down(status_d, node)
        else:
            # there was an error, mark enstore as down
            mark_enstore_down(status_d, node)
            
    # now create the web page
    filename = "%s/%s"%(html_dir, enstore_constants.STATUSONLYHTMLFILE)
    only_file = enstore_files.HtmlStatusOnlyFile(filename)
    only_file.open()
    only_file.write(status_d, nodes)
    only_file.close()
    only_file.install()


if __name__ == "__main__" :

    do_work()
