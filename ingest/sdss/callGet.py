import os
import sys
import tempfile
import popen2
import string
import types

def callGet(tapeLabel, files, pnfsDir, outputDir, verbose):
    pnfsd_s = os.path.split(pnfsDir)
    if pnfsd_s[len(pnfsd_s)-1] != tapeLabel:
        pnfsDir = os.path.join(pnfsDir,tapeLabel)
    output_s = os.path.split(outputDir)
    out_is_null = 0
    # aalow output to go to /dev/null
    if len(output_s) == 2 and output_s[0].find("/dev") != -1 and output_s[1].find("null") != -1:
        out_is_null = 1
        pass
    else:
        if output_s[len(output_s)-1] != tapeLabel:
            outputDir = os.path.join(outputDir,tapeLabel)
    #print "pnfsd %s out_d %s"%(pnfsDir, outputDir)
    
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

    #Detemine which path(s) exists.
    if enstore_path and os.path.exists(enstore_path):
        sys.stderr.write("Warning: Using enstore version of get.\n")
        path = enstore_path
    elif sdsscp_path and os.path.exists(sdsscp_path):
        path = sdsscp_path
    else:
        sys.stderr.write("Unable to find get executable.\n")
        os.remove(fname)
        sys.exit(70)
        
    if not out_is_null:
        os.system("mkdir -p %s"%(outputDir,))
    os.system("mkdir -p %s"%(pnfsDir,))
    if (not os.path.exists(pnfsDir)):
        sys.stderr.write("%s does not exist\n"%(pnfsDir,))
        os.remove(fname)
        sys.exit(70)
    if (not os.path.exists(outputDir)):
        sys.stderr.write("%s does not exist\n"%(outputDir,))
        os.remove(fname)
        sys.exit(70)
    
    while 1:
        if verbose:
            vopt = "--verbose %s"%(verbose,)
        else:
            vopt = " "
        args = (path, vopt, "--bypass-filesystem-max-filesize-check",
                "--list", fname, tapeLabel, pnfsDir, outputDir)

        print string.join(args), "\n"

        #Fork off the "get" process and read in its output.

        missingFiles = []
        pipeObj = popen2.Popen4(string.join(args),  0)
        if pipeObj is None:
            print "could not fork off the process %s" % (args,)
            os.remove(fname)
            return 1

        rc = -1 #poll() returns -1 when the process is still alive.
        line = 1 #Somethig true.
        while rc == -1 or line:
            line = pipeObj.fromchild.readline()
            #if rc == -1:
            #    rc = pipeObj.poll() # Don't get if we already have it.
            if line:
                #Remove trailing newline.
                line = line[:-1]
                #This will put all of the standard out and err output from
                # get to sdsscp's standard out.  There is probably a better
                # way, but for now this will work.
                print line
                sys.stdout.flush()
                if line.find("error_output") != -1:
                    items = line.split()
                    try:
                        missingFiles.append(items[1])
                    except IndexError, detail:
                        sys.stderr.write("%s\n" % (detail,))
            else:
                #Get the exit status when the pipe has been closed.
                #if rc == -1:
                rc = pipeObj.wait()
                break

        rc = rc >> 8 #This needs to be shifted 8.
        if rc == 0 or rc == 2:
            break

        del(pipeObj)
        print "missing files", missingFiles
        if missingFiles:
            old_fname = fname
            fname = tempfile.mktemp()
            print "new list is", fname
            f = open(fname, "w")
            oldf = open(old_fname, "r")
            while 1:
                l = oldf.readline()[:-1]
                if l:
                    f_number, f_name = l.split()
                    if f_number in missingFiles:
                        f.write("%s %s\n"%(f_number, f_name))
                else:
                    break
            oldf.close()
            f.close()
            os.remove(old_fname)
        else:
            break

        print "will retry"
    
    #Cleanup the temporary file.
    os.remove(fname)
    return rc
    
    #Print out messages stating any failures that occured.
    if missingFiles:
        sys.stderr.write("The following files were requested, "
                         "but not delivered.\n")
        for missingFile in missingFiles:
            sys.stderr.write("file: %s\n" % missingFile)
        return 1

    return rc
