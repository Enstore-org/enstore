import os

#Get the file contianing the metadata for the tapetype/volume filename.

def getFile(username, filename, mjd, nodename, metadata_output):
    try:
        #Create the full remote file path to the sdss catalog metadata.
        source_filename = username + "@" + nodename + ":/sdss/data/golden/" \
                          + mjd + "/" + filename
        #Get the medatada file from sdss.
        return os.spawnvp(os.P_WAIT, "rcp",
                          ("rcp", source_filename, metadata_output))
    except:
        print "unable to fetch file"
        return -1

