import os
import sys
import string

#Get the file contianing the metadata for the tapetype/volume filename.

def getFile(username, filename, mjd, nodename, metadata_output):
    try:
        #Create the full remote file path to the sdss catalog metadata.
        source_filename = username + "@" + nodename + ":/sdss/data/golden/" \
                          + mjd + "/" + filename
        print string.join(("rcp", source_filename, metadata_output), " ")
        sys.stdout.flush()
        #Get the medatada file from sdss.
        return os.spawnvp(os.P_WAIT, "rcp",
                          ("rcp", source_filename, metadata_output))
    except:
        msg = sys.exc_info()[1]
        print "unable to fetch file: %s" % str(msg)
        return -1

