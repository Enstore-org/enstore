import os
import getopt
import sys
import string
import re

def fix_cgi_url(web_dir, file, url, extra_dir=""):
    # edit the named file and change the url for the cgi script to the value in 'url'
    # we will need the name of the file to construct the name of the cgi file
    (filename, ext) = string.split(file, ".")
    file = "%s/%s"%(web_dir, file)

    #define target and replacement patterns
    target = re.compile("ACTION=\"[^\"]+\"")
    replacement = "ACTION=\"%s/enstore/%s%s_cgi.py\""%(url, extra_dir, filename)
    
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

def get_inputs():
    # see if anything was passed to us on the command line.  if not prompt 
    # the user for the information.
    try:
        options = ["web_dir=", "cgi_url="]
        optlist, args = getopt.getopt(sys.argv[1:], "", options)
    except getopt.error, detail:
        print "error: ", detail
        sys.exit(1)

    web_dir = ""
    cgi_url = ""
    if not len(optlist) == 0:
        # we have some input
        for (opt, value) in optlist:
            if opt == "--web_dir":
                web_dir = value
            elif opt == "--cgi_url":
                cgi_url = value
    else:
        # nothing entered on the command line, assume a default
        web_dir = "/fnal/ups/prd/www_pages/enstore"
        cgi_url = "http://www-d0en.fnal.gof:/cgi-bin"

    return(web_dir, cgi_url)

if __name__ == "__main__" :
    
    # make sure enstore has been set up
    if os.environ.get("ENSTORE_DIR", ""):
	# process inputs, if there are any
	(web_dir, cgi_url) = get_inputs()

	# edit the html files to set the location of the cgi url
	if  cgi_url:
	    print "Changing the url for the cgi scripts to %s"%(cgi_url,)
	    fix_cgi_url(web_dir, "enstore_user.html", cgi_url)
	    fix_cgi_url(web_dir, "enstore_log_file_search.html", cgi_url, "log/")
	    fix_cgi_url(web_dir, "enstore_alarm_search.html", cgi_url)
