import string
import sys

def makeTapelogFilename(run, frame, ccd):
    #return "idTapeLog-%06d-%02d-%04d.fit"%(int(run), int(ccd), int(frame))
    #return "idTapeLog-00" + run + "-" + ccd + "-000" + frame + ".fit"

    #The ccd should always be a 2 digit number.
    if len(str(ccd)) != 2:
        sys.stderr.write("Length of ccd is not 2 but %s." % len(string(ccd)))
        sys.exit(1)
    #The first digit is the filter (sometimes refered to as "camrow" in
    # the internal SDSS code), the second digit is the camera column.
    ccd_filter = int(ccd[0])
    ccd_camcol = int(ccd[1])

    #The filter needs to be converted to is letter counterpart.  Map 1-5
    # to the tuple's 0-4.
    try:
        filter_conv = ("r", "i", "u", "z", "g")
        converted_filter = filter_conv[ccd_filter - 1]
    except IndexError:
        #The valid range of filter is [1 - 5].
        sys.stderr.write("The filter value (%s) is invalid." % ccd_filter)
        sys.exit(1)

    if ccd_camcol < 1 or ccd_camcol > 6:
        #The valid range of camcol is [1 - 6].
        sys.stderr.write("The camcol value (%s) is invalid." % ccd_camcol)
        sys.exit(1)

    #Need to convert the frame to the field value.  (Illegal filter values
    # should already have been found.)  Map 1-6 to the tuple's 0-5.
    iframe = int(frame)
    field_conv = (iframe - 0, iframe - 2, iframe - 4, iframe - 6, iframe - 8)
    field = field_conv[ccd_filter - 1]

    return "idR-%06d-%1s%1d-%04d.fit" % (int(run), converted_filter,
                                         int(ccd_camcol), int(field))
    
def makeTarlogFilename(contents, id):
    return string.lower(contents) + "." + id + ".tar"

def parseFile(filename):   #, tapeLabel):
    print "FILE",filename
    filelist = []
    f = open(filename)
    line = f.readline()
    while line:
        if not line.lower().find("tapelog"):
            #Split tapelog lines contain tuples of the following:
            # (tape, filemark, run, frame, ccd)
            (unused, filemark, run, frame, ccd) = line.split()
            filelist.append((filemark,
                             makeTapelogFilename(run, frame, ccd)))
        elif not line.lower().find("tarfile"):
            #Split TARFILE lines contain tuples of the following:
            # (tape, filemark, contents, tar_id)
            (unused, filemark, contents, tar_id) = line.split()
            filelist.append((int(filemark) + 1,
                             makeTarlogFilename(contents, tar_id)))
        #else:
        #    sys.stderr.write("Invalid tape type found.\n")
        #    sys.exit(50)
            
        line = f.readline()

    f.close()  #Cleanup
    return filelist
    


