import os
import tempfile

def callGet(tapeLabel, files, pnfsDir, outputDir):
    fname = tempfile.mktemp()
    f = open(fname,"w")

    for fileentry in files:
        #Add one to compensate for double filemark bug on sdss
        #volumes.  File one in meta data is file two on tape
        print fileentry
        
        f.write( fileentry[0]) + " " + fileentry[1] + "\n")

    f.close()

    #args = ("python", "/home/wellner/dev/enstore/src/get.py", "--verbose", "1", "--list", fname, tapeLabel,pnfsDir, outputDir)
    args = ("python", "/home/wellner/dev/enstore/src/get.py", "--list", fname, tapeLabel,pnfsDir, outputDir)

    print "python", args
    return os.spawnvp(os.P_WAIT, "python", args)
