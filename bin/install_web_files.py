# this file will install the web pages and cgi scripts for the enstore product.  it needs to be run
# on the node where the web server is run. the following steps are performed - 
#
#     o make sure enstore has been set up
#     o prompt the user for the following information -
#                 + directory specification of the cgi directory
#                 + directory specification of the web page directory
#                 + web location of the cgi directory  (e.g. - http://www-d0en.fnal.gov:/cgi-bin/)
#     o copy $ENSTORE_DIR/etc/*.html files to the web page area
#     o copy $ENSTORE_DIR/etc/*.gif files to the web page area
#     o edit enstore_user.html in the web page area to fix the cgi node specification
#     o edit enstore_log_file_search.html in the web page area to fix the cgi node specification
#     o edit enstore_alarm_search.html in the web page area to fix the cgi node specification
#
#   ( the following cgi steps may need to be performed while logged in as the www server user
#           (e.g. wsrvd0en))
#
#     o make an enstore subdirectory in the cgi-bin area
#     o copy $ENSTORE_DIR/src/enstore_*_cgi.py to the cgi-bin/enstore area
#     o copy $ENSTORE_DIR/src/enstore.htaccess to cgi-bin/enstore/.htaccess
#     o inform the user that they may need to edit the .htaccess file appropriately
#
#     o warn the user that there may need to be a link to the python executable in /usr/local/bin
#
# this script is designed to be able to be run several times without overwriting what was done in a
# previous step.  checks are made before any step is executed.  this this script can be run as user
# enstore to install the html files and then as user wsrvd0en (for example) in order to install the
# cgi scripts.

# system imports
import getopt
import sys
import os
import fileinput
import re
import string

def get_from_user(prompt):
    # prompt the user for input
    prompt_format = "Enter the %s : "
    return(raw_input(prompt_format%(prompt,)))

def get_inputs():
    # see if anything was passed to us on the command line.  if not prompt the user for the information.
    try:
	options = ["cgi_dir=", "web_dir=", "cgi_url="]
	optlist, args = getopt.getopt(sys.argv[1:], "", options)
    except getopt.error, detail:
	print "error: ", detail
	sys.exit(1)

    if not len(optlist) == 0:
	cgi_dir = ""
	web_dir = ""
	cgi_url = ""
	# we have some input
	for (opt, value) in optlist:
	    if opt == "--cgi_dir":
		cgi_dir = value
	    elif opt == "--web_dir":
		web_dir = value
	    elif opt == "--cgi_url":
		cgi_url = value
    else:
	# nothing entered on the command line, we must prompt the user
	cgi_dir = get_from_user("cgi bin directory")
	web_dir = get_from_user("html web files directory")
	cgi_url = get_from_user("cgi bin web url")

    return(cgi_dir, web_dir, cgi_url)

def copy_files(files, dir):
    # copy 'files' to 'dir'
    os.system("cp %s %s"%(files, dir))

def fix_cgi_url(web_dir, file, url):
    # edit the named file and change the url for the cgi script to the value in 'url'
    # we will need the name of the file to construct the name of the cgi file
    (filename, ext) = string.split(file, ".")
    file = "%s/%s"%(web_dir, file)

    #define target and replacement patterns
    target = re.compile("ACTION=\"[^\"]+\"")
    replacement = "ACTION=\"%s/enstore/%s_cgi.py\""%(url, filename)
    
    #get at the source text
    infile = open(file, 'r')
    lines = infile.readlines()
    infile.close()

    #do the replacement
    lines2 = []
    for line in lines:
	lines2.append(re.sub(target, replacement, line))

    #write the output
    outfile = open(file, 'w')
    outfile.writelines(lines2)
    outfile.close()


def make_subdir(subdir):
    # create the entered directory if it does not exist
    try:
	os.stat(subdir)
    except OSError:
	print "Making %s"%(enstore_cgi,)
	os.mkdir(subdir)

if __name__ == "__main__" :
    
    # make sure enstore has been set up
    if os.environ.get("ENSTORE_DIR", ""):
	# process inputs, if there are any
	(cgi_dir, web_dir, cgi_url) = get_inputs()

	# copy the html and gif files to the web page area
	if web_dir:
	    print "Copying html and gif files to %s"%(web_dir,)
	    copy_files("$ENSTORE_DIR/etc/*.html", web_dir)
	    copy_files("$ENSTORE_DIR/etc/*.gif", web_dir)

	    # edit the html files to set the location of the cgi url
	    if  cgi_url:
		print "Changing the url for the cgi scripts to %s"%(cgi_url,)
		fix_cgi_url(web_dir, "enstore_user.html", cgi_url)
		fix_cgi_url(web_dir, "enstore_log_file_search.html", cgi_url)
		fix_cgi_url(web_dir, "enstore_alarm_search.html", cgi_url)
		
	# make an enstore subdir in the cgi area
	if cgi_dir:
	    enstore_cgi = "%s/enstore"%(cgi_dir,)
	    make_subdir(enstore_cgi)

	    # copy the cgi scripts to the enstore cgi area
	    print "Copying the cgi scripts to %s"%(enstore_cgi,)
	    copy_files("$ENSTORE_DIR/src/enstore_*_cgi.py", enstore_cgi)

	    # copy the .htaccess file
	    print "Copying the htaccess file to %s"%(enstore_cgi,)
	    copy_files("$ENSTORE_DIR/etc/enstore.htaccess", "%s/.htaccess"%(enstore_cgi,))
	    print "NOTE: You may need to edit the %s/.htaccess file to correctly set access to the Enstore cgi scripts."%(enstore_cgi,)

	# check if /usr/local/bin/python exists, if not warn user it may need to
	try:
	    os.stat("/usr/local/bin/python")
	except OSError:
	    print "NOTE: The cgi scripts will need to be able to find python in a well known location. (e.g. /usr/local/bin)"

