import string
import sys

def makeTapeLogFilename(run, frame, ccd):
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
    
def makeTarTapeFilename(contents, id):
    return string.lower(contents) + "." + id + ".tar"

def makePtTapeFilename(mjd, mjd_count):
    if mjd_count:
        return mjd + "_c" + str(mjd_count) + ".tar"
    
    return mjd + ".tar"

def parseTarTapeParFile(filename):
    filelist = []
    f = open(filename)
    line = f.readline()
    while line:
        if (line.lower().find("tarfile") >= 0):
            #Split "tarfile" lines containing tuples of the following:
            # (tape, filemark, contents, tar_id)
            try:
                (unused, filemark, contents, tar_id) = line.split()
                filelist.append((int(filemark) + 1,
                                 makeTarTapeFilename(contents, tar_id)))
            except ValueError:
                pass

        line = f.readline()

    f.close()  #Cleanup
    return filelist
    
def parseTapeLogParFile(filename):
    filelist = []
    f = open(filename)
    line = f.readline()
    while line:
        if line.lower().find("tapelog") >= 0:
            #Split "tapelog" lines containing tuples of the following:
            # (tape, filemark, run, frame, ccd)
            try:
                (unused, filemark, run, frame, ccd) = line.split()
                filelist.append((filemark,
                                 makeTapeLogFilename(run, frame, ccd)))
            except ValueError:
                pass

        line = f.readline()

    f.close()  #Cleanup
    return filelist

def parsePtTapeTapelogFile(filename):
    tapelabel = None
    fma = 0  #File Mark Adjustment (0 or -1)
    sfm = 2 #Skip file marks (1 or 2)
    mjd_list = []
    
    filelist = []
    f = open(filename)
    line = f.readline()
    while line:

        words = line.split()

        if words[:3] == ["Tape", "Label", "="]:
            tapelabel = words[3]

            #Determine if this is an older tape that needs a file mark
            # adjustment to read the correct location.
            # Note: PtTape tapes after JL0133 have two filemarks between
            #       every file.  Hence the need to skip 2 filemarks.
            # Note2: PtTape tapes after JL0133 have the actual files
            #        on the odd or even locations, depending where the
            #        tape falls in the range.
            #        * 133 through 139 and starting with 1897 are evens.
            #        + 136 and 160 through 1887 are odds.
            # sfm == 1 and fma == 0 implies files at locations 1, 2, 3...
            #
            # sfm == 2 and fma == -1 implies files at locations 1, 3, 5...
            #
            # sfm == 2 and fma == 0 implies files at locations 2, 4, 6...
            if tapelabel[:2] == "JL":
                tape_number = int(tapelabel[2:])
                if 61 <= tape_number and tape_number <= 133:
                    sfm = 1
                    fma = 0
                elif 136 <= tape_number and tape_number <= 136:
                    sfm = 2
                    fma = -1
                elif 139 <= tape_number and tape_number <= 159:
                    sfm = 2
                    fma = 0
                elif 160 <= tape_number and tape_number <= 349:
                    sfm = 2
                    fma = -1
                elif 902 <= tape_number and tape_number <= 1887:
                    sfm = 2
                    fma = -1
                elif 1897 <= tape_number and tape_number <= 2153:
                    sfm = 2
                    fma = 0
                elif 2245 <= tape_number and tape_number <= 2334:
                    sfm = 2
                    fma = -1
                elif 2335 <= tape_number and tape_number <= 2380:
                    sfm = 2
                    fma = 0
                elif 2411 <= tape_number and tape_number <= 3251:
                    sfm = 2
                    fma = -1
                elif 3331 <= tape_number and tape_number <= 3338:
                    sfm = 2
                    fma = 0
                elif 3339 <= tape_number and tape_number <= 3339:
                    sfm = 2
                    fma = -1
                elif 3341 <= tape_number and tape_number <= 3341:
                    sfm = 2
                    fma = 0
                elif 3342 <= tape_number and tape_number <= 3373:
                    sfm = 2
                    fma = -1
                elif 3375 <= tape_number and tape_number <= 3375:
                    sfm = 2
                    fma = 0
                elif 3379 <= tape_number and tape_number <= 3379:
                    sfm = 2
                    fma = -1
                elif 3413 <= tape_number and tape_number <= 3492:
                    sfm = 2
                    fma = 0
                elif 3495 <= tape_number and tape_number <= 3505:
                    sfm = 2
                    fma = -1
                elif 3520 <= tape_number and tape_number <= 3635:
                    sfm = 2
                    fma = 0
                elif 3659 <= tape_number and tape_number <= 3661:
                    sfm = 2
                    fma = -1
                elif 3690 <= tape_number and tape_number <= 3820:
                    sfm = 2
                    fma = 0
                elif 3823 <= tape_number and tape_number <= 3823:
                    sfm = 2
                    fma = -1
                elif 3826 <= tape_number and tape_number <= 3908:
                    sfm = 2
                    fma = 0
                elif 3953 <= tape_number and tape_number <= 3966:
                    sfm = 2
                    fma = -1
                elif 4021 <= tape_number and tape_number <= 4480:
                    sfm = 2
                    fma = 0
                elif 4487 <= tape_number and tape_number <= 4496:
                    sfm = 2
                    fma = -1
                elif 4535 <= tape_number and tape_number <= 4561:
                    sfm = 2
                    fma = 0
                elif 4564 <= tape_number and tape_number <= 4568:
                    sfm = 2
                    fma = -1
                elif 4571 <= tape_number and tape_number <= 4610:
                    sfm = 2
                    fma = 0
                elif 4613 <= tape_number and tape_number <= 4617:
                    sfm = 2
                    fma = -1
                elif 4656 <= tape_number and tape_number <= 4709:
                    sfm = 2
                    fma = 0
                elif 4712 <= tape_number and tape_number <= 4712:
                    sfm = 2
                    fma = -1
                elif 4715 <= tape_number and tape_number <= 4906:
                    sfm = 2
                    fma = 0
                elif 4935 <= tape_number and tape_number <= 4935:
                    sfm = 2
                    fma = -1
                elif 4950 <= tape_number and tape_number <= 5084:
                    sfm = 2
                    fma = 0
                elif 5099 <= tape_number and tape_number <= 5099:
                    sfm = 2
                    fma = -1
                elif 5114 <= tape_number and tape_number <= 6240:
                    sfm = 2
                    fma = 0
                elif 6247 <= tape_number and tape_number <= 6255:
                    sfm = 2
                    fma = -1
                elif 6290 <= tape_number and tape_number <= 7367:
                    sfm = 2
                    fma = 0
                elif 7446 <= tape_number and tape_number <= 7446:
                    sfm = 1
                    fma = 0
                elif 7449 <= tape_number and tape_number <= 8768:
                    sfm = 2
                    fma = 0
                else:
                    sys.stderr.write("%s tape layout unknown\n" % tapelabel)
                    sys.exit(1)
            else:
                #We don't have a JL tape.  Perhapse a JG tape?
                sys.stderr.write("%s tape layout unknown\n" % tapelabel)
                sys.exit(1)
                
        elif words[:2] == ["Data", "="] or \
                 (len(words) == 1 and words[0].is_digit):
            #Split "Data =" lines containing tuples of the following:
            # (mjd,)
            #Another type of line is one just containing a number for
            # the mjd.
            try:
                if words[:2] == ["Data", "="]:
                    mjd = words[2]
                else:
                    mjd = words[0]

                #The mjd_list code is necessary to determine if the same
                # tarfile (at least in name) is written to the tape more
                # than once.
                mjd_count = mjd_list.count(mjd)
                mjd_list.append(mjd)

                file_location = ((len(filelist) + 1) * sfm) + fma
                
                filelist.append( (file_location,
                                  makePtTapeFilename(mjd, mjd_count)) )
            except ValueError:
                pass

        line = f.readline()

    f.close()  #Cleanup
    return filelist

def parseFile(filename, tapeStyle):

    if tapeStyle == "TarTape":
        return parseTarTapeParFile(filename)
    elif tapeStyle == "TapeLog":
        return parseTapeLogParFile(filename)
    elif tapeStyle == "PtTape":
        return parsePtTapeTapelogFile(filename)
    
    sys.stderr.write("%s tape style unknown\n" % tapeStyle)
    sys.exit(1)

    #Never reach here.  Just shuts pychecker up.
    return [] 
