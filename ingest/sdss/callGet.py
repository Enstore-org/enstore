import os
import tempfile

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
        path = path + "/bin/get.py"
    else:
        path = os.getenv("SDSS_DIR")
        if not path:
            print "SDSS_DIR not set, can't find get.py"
            return -2
        else:
            path = path + "/get.py"
        
    #args = ("python", path, "--verbose", "1", "--list", fname, tapeLabel,pnfsDir, outputDir)
    args = ("python", path, "--list", fname, tapeLabel,pnfsDir, outputDir)

    print "python", args
    return os.spawnvp(os.P_WAIT, "python", args)
