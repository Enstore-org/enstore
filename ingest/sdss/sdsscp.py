#!/usr/bin/env python

import callGet
import string, sys
import parseTapeLog
import getNames

version = string.split(string.split(sys.version)[0], ".")

if map(int, version) < [1, 6, 0]:
    print "must use python 1.6 or greater"
    sys.exit(127)

if len(sys.argv) != 6:
    print sys.argv[0], " <tapelabel> <mjd> <TarTape|TapeLog> <pfnsdir> <outputdir>"
    sys.exit(126)

tapeLabel = sys.argv[1]
mjd = sys.argv[2]
tapeStyle = sys.argv[3]
pnfsDir = sys.argv[4]
outputDir = sys.argv[5]

if tapeStyle != "TarTape" and tapeStyle != "TapeLog":
    print "tape style must be TapeLog or TarTape"
    sys.exit(125)

metaFileName = "id"+tapeStyle+"-"+tapeLabel+".par"

if getNames.getFile("sdssdp",metaFileName,mjd,"sdssdp30") < 0:
    sys.exit(-1)

files = parseTapeLog.parseFile(metaFileName, tapeLabel)
sys.exit(callGet.callGet(tapeLabel, files, pnfsDir, outputDir))
