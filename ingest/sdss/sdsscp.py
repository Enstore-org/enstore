#!/usr/bin/env python

import os
import getopt

import callGet
import string, sys
import parseTapeLog
import getNames

def main():
    #Make sure that a new enough version of python is used.
    version = string.split(string.split(sys.version)[0], ".")
    if map(int, version) < [1, 6, 0]:
        print "must use python 1.6 or greater"
        sys.exit(127)

    opts_permitted = ['verbose=']
    options, args =getopt.getopt(sys.argv[1:], [], opts_permitted)
    #If the wrong number of arguments was supplied, print help and error out.
    if len(args) != 5:
        print "Usage:",sys.argv[0], \
              " [--verbose n] <tapelabel> <mjd> <TarTape|TapeLog> <pfnsdir> <outputdir>"
        sys.exit(126)

    verbose = None
    if options:
        for option in options:
            if "--verbose" in option:
                verbose = option[1]
        
    #Create shortcuts for readability.
    tapeLabel = args[0]
    mjd = args[1]
    tapeStyle = args[2]
    pnfsDir = args[3]
    outputDir = args[4]

    #Detect tape type errors from the command line.
    if tapeStyle != "TarTape" and tapeStyle != "TapeLog":
        print "tape style must be TapeLog or TarTape"
        sys.exit(125)

    #Build the "par" filename.
    metaFileName = "id" + tapeStyle + "-" + tapeLabel + ".par"
    #Set the fullpath of this file.
    localMetaFilePath = os.path.join("/tmp", metaFileName)
    print "localMetaFilePath", localMetaFilePath

    #Copy the catalog metadata file to the /tmp directory.
    if getNames.getFile("sdssdp", metaFileName, mjd,
                        "sdssdp30", localMetaFilePath) < 0:
        if os.path.exists(localMetaFilePath):
            os.remove(localMetaFilePath)
        sys.exit(124)

    #Read in the metadata catalog file just copied over.
    files = parseTapeLog.parseFile(localMetaFilePath)  #, tapeLabel)

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
    
    #Fork off "get" process to retrieve the data.
    exit_status = callGet.callGet(tapeLabel, uncopied_files, pnfsDir,
                                  outputDir, verbose)

    #The copied catalog file is removed at this point.
    os.remove(localMetaFilePath)

    print "Exit status = %s." % exit_status
    sys.exit(exit_status)

if __name__ == '__main__':
    main()
