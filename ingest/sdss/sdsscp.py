#!/usr/bin/env python

import os

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

    #If the wrong number of arguments was supplied, print help and error out.
    if len(sys.argv) != 6:
        print sys.argv[0], \
              " <tapelabel> <mjd> <TarTape|TapeLog> <pfnsdir> <outputdir>"
        sys.exit(126)

    #Create shortcuts for readability.
    tapeLabel = sys.argv[1]
    mjd = sys.argv[2]
    tapeStyle = sys.argv[3]
    pnfsDir = sys.argv[4]
    outputDir = sys.argv[5]

    #Detect tape type errors from the command line.
    if tapeStyle != "TarTape" and tapeStyle != "TapeLog":
        print "tape style must be TapeLog or TarTape"
        sys.exit(125)

    #Build the "par" filename.
    metaFileName = "id" + tapeStyle + "-" + tapeLabel + ".par"
    #Set the fullpath of this file.
    localMetaFilePath = os.path.join("/tmp", metaFileName)

    #Copy the catalog metadata file to the /tmp directory.
    if getNames.getFile("sdssdp", metaFileName, mjd,
                        "sdssdp30", localMetaFilePath) < 0:
        sys.exit(124)

    #Read in the metadata catalog file just copied over.
    files = parseTapeLog.parseFile(localMetaFilePath)  #, tapeLabel)

    #Fork off "get" process to retrieve the data.
    exit_status = callGet.callGet(tapeLabel, files, pnfsDir, outputDir)

    #The copied catalog file is removed at this point.
    os.remove(localMetaFilePath)

    print "Exit status = %s." % exit_status
    sys.exit(exit_status)

if __name__ == '__main__':
    main()
