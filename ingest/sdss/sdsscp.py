#!/usr/bin/env python

import os
import getopt
import errno
import re
import string
import sys

import callGet
import parseTapeLog
import getNames

def main():
    #Make sure that a new enough version of python is used.
    version = string.split(string.split(sys.version)[0], ".")
    if map(int, version) < [1, 6, 0]:
        sys.stderr.write("must use python 1.6 or greater\n")
        sys.exit(127)

    opts_permitted = ['verbose=', "dump"]
    options, args = getopt.getopt(sys.argv[1:], [], opts_permitted)
    #If the wrong number of arguments was supplied, print help and error out.
    if len(args) != 5:
        sys.stderr.write("Usage: %s [--verbose n] [--dump] "
                         "<tapelabel> <mjd|ts_dir> <TarTape|TapeLog|PtTape> " \
                         "<pfnsdir> <outputdir>\n" % sys.argv[0])
        sys.exit(126)

    verbose = None
    dump = None
    if options:
        for option in options:
            if "--verbose" in option:
                verbose = option[1]
            if "--dump" in option:
                dump = True
                
    #Create shortcuts for readability.
    tapeLabel = args[0]
    mjd = args[1]
    tapeStyle = args[2]
    pnfsDir = args[3]
    outputDir = args[4]

    USERNAME = "sdssdp"
    NODENAME = "sdssdp30"
    DIRNAME = "/sdss/data/golden/"
    PTNODENAME = "sdssdp40"
    PTDIRNAME = "/data/dp40.c/data/mt/tapelogs"

    #Detect tape type errors from the command line.
    if tapeStyle != "TarTape" and tapeStyle != "TapeLog" \
           and tapeStyle != "PtTape":
        sys.stderr.write("Tape style must be TapeLog, PtTape or TarTape.\n")
        sys.exit(125)
    #Detect tape name errors from the command line.
    if re.match("J[GL][A-Z0-9]{2}[0-9]{2}", tapeLabel) == None:
        sys.stderr.write("%s is not a valid tape name.\n" % tapeLabel)
        sys.exit(124)

    #Check credentials.
    if os.system("klist -s"):
        sys.stderr.write("Failed to find valid credentails.\n")
        sys.exit(123)

    #Determine the par filepath.
    if tapeStyle == "TarTape" or tapeStyle == "TapeLog":
        par_filepath = getNames.getFilename(USERNAME, NODENAME, DIRNAME,
                                            mjd, tapeStyle, tapeLabel)
    else:  #PtTape
        #Note: in this case mjd really means the telescope directory.
        # Either: apo20, apo24 or usno
        par_filepath = getNames.getFilename(USERNAME, PTNODENAME, PTDIRNAME,
                                            mjd, tapeStyle, tapeLabel)
    #Determine the local file to copy the .par file to.
    localMetaFilePath = os.path.join("/tmp", os.path.basename(par_filepath))

    #Copy the catalog metadata file to the /tmp directory.
    if getNames.getFile(par_filepath, localMetaFilePath):

        #Finding the file will take a while.
        par_file = getNames.findFile(USERNAME, NODENAME, DIRNAME, tapeLabel)

        if par_file == None:
            #Before giving up, check the PT directories.
            par_file = getNames.findFile(USERNAME, PTNODENAME,
                                         PTDIRNAME, tapeLabel)

        if par_file == None:
            #getNames.findFile has printed the error message.
            sys.exit(122)

        #Get the par file with the name from find(1).
        if getNames.getFile(par_file, localMetaFilePath):
            sys.stderr.write("Unable to get .par file for volume %s.\n"
                             % tapeLabel)
            sys.exit(121)

    #Read in the metadata catalog file just copied over.
    files = parseTapeLog.parseFile(localMetaFilePath, tapeStyle)

    if dump:
        callGet.writeGetFile(files, file_obj = sys.stdout)
        sys.exit(0)

    #Shrink the list to remove files already copied.  If the output is
    # /dev/null, this step is unecessary.
    if outputDir == "/dev/null":
        uncopied_files = files
    else:
        uncopied_files = []
        for item in files:
            if not os.path.exists(os.path.join(outputDir, item[1])):
                uncopied_files.append(item)
            else:
                print "File", item, "already copied.  Skipping."

    if uncopied_files:
        #Fork off "get" process to retrieve the data.
        exit_status = callGet.callGet(tapeLabel, uncopied_files, pnfsDir,
                                      outputDir, verbose)
    else:
        exit_status = 0

    #The copied catalog file is removed at this point.
    try:
        os.remove(localMetaFilePath)
    except OSError, msg:
        if getattr(msg, "errno", None) == errno.ENOENT:
            #The file is already gone.
            pass
        else:
            sys.stderr.write("Unable to remove temporary file %s: %s\n" % \
                             (localMetaFilePath, str(msg)))

    print "Exit status = %s." % exit_status
    sys.exit(exit_status)

if __name__ == '__main__':
    try:
        main()
    except (KeyboardInterrupt), msg:
        sys.stderr.write("\nUser aborted sdsscp.\n")
        sys.exit(1)
