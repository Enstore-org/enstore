import os
import sys
import tempfile
import popen2
import string

def callGet(tapeLabel, files, pnfsDir, outputDir):
    fname = tempfile.mktemp()
    f = open(fname, "w")

    for fileentry in files:
        #Add one to compensate for double filemark bug on sdss
        #volumes.  File one in meta data is file two on tape
        ###1-6-2004: MWZ something isn't correct here.  Comment and code
        ### do not match.
        f.write(str(fileentry[0]) + " " + fileentry[1] + "\n")

    f.close()

    #Must read ENSTORE_DIR first per mike Z's passionate opinion.
    #This should be changed to only look at SDSSCP_DIR at some point

    try:
        enstore_path = os.path.join(os.getenv("ENSTORE_DIR"), "bin/get")
    except TypeError:
        enstore_path = None
    try:
        sdsscp_path = os.path.join(os.getenv("SDSSCP_DIR"), "get")
    except TypeError:
        sdsscp_path = None

    #Detemine which path(s) exits.
    if enstore_path and os.path.exists(enstore_path):
        sys.stderr.write("Warning: Using enstore version of get.\n")
        path = enstore_path
    elif sdsscp_path and os.path.exists(sdsscp_path):
        path = sdsscp_path
    else:
        sys.stderr.write("Unable to find get executable.\n")
        sys.exit(70)
        
    #args = (path, "--verbose", "4", "--list", fname, tapeLabel,
    #        pnfsDir, outputDir)
    args = (path, "--list", fname, tapeLabel, pnfsDir, outputDir)

    print string.join(args)

    #Fork off the "get" process and read in its output.
    standard_in, standard_out_err = os.popen4(args)
    missingFiles = []
    line = standard_out_err.readline()
    while line:
        sys.stdout.write(line)
        if line[:17] == "unable to deliver":
            missingFiles.append(line[19:])
        line = standard_out_err.readline()

    #Cleanup of open files.
    standard_out_err.close()
    standard_in.close()
    #Cleanup the temporary file.
    os.remove(fname)
    
    #Print out messages stating any failures that occured.
    if missingFiles:
        sys.stderr.write("The following files were requested, "
                         "but not delivered.\n")
        for missingFile in missingFiles:
            sys.stderr.write("file: %s\n" % missingFile)
        return 1

    return 0
    #return os.spawnvp(os.P_WAIT, "python", args)
