import os
import sys
import tempfile
import popen2

def callGet(tapeLabel, files, pnfsDir, outputDir):
    fname = tempfile.mktemp()
    f = open(fname,"w")

    for fileentry in files:
        #Add one to compensate for double filemark bug on sdss
        #volumes.  File one in meta data is file two on tape
        f.write( str(fileentry[0]) + " " + fileentry[1] + "\n")

    f.close()

    #Must read ENSTORE_DIR first per mike Z's passionate opinion.
    #This should be changed to only look at SDSS_DIR at some point
    
    path=os.getenv("ENSTORE_DIR")
    if path:
        path = os.path.join(path, "bin/get")
    else:
        path = os.getenv("SDSS_DIR")
        if not path:
            print "SDSS_DIR not set, can't find get."
            return -2
        else:
            path = os.path.join(path , "get")

    #args = ("python", path, "--verbose", "1", "--list", fname, tapeLabel,pnfsDir, outputDir)
    args = ("python", path, "--list", fname, tapeLabel,pnfsDir, outputDir)

    print "python", args

    standard_out, standard_in, standard_err = popen2.popen3(args)
    missingFiles = []
    line = standard_err.readline()
    while line:
        if line[:17] == "unable to deliver":
            missingFiles.append(line[19:])
        line = standard_err.readline()

    if missingFiles:
        sys.stderr.write("The following files were requested, "
                         "but not delivered.\n")
        for missingFile in missingFiles:
            sys.stderr.write("file: %s\n" % missingFile)
        
    #return os.spawnvp(os.P_WAIT, "python", args)
