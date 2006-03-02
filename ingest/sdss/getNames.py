import os
import sys
import string


def getFilename(username, nodename, dirname, mjd, tapeStyle, tapeLabel):

    if tapeStyle == "PtTape":
        #Build the PtTape tapelog filename.
        metaFileName = tapeLabel + ".tapelog"
    else:
        #Build the TarTape or TapeLog "par" filename.
        metaFileName = "id" + tapeStyle + "-" + tapeLabel + ".par"

    #Set the fullpath of this file.
    remoteMetaFilePath = os.path.join(dirname, mjd, metaFileName)
    #Create the full remote file path to the sdss catalog metadata.
    source_filename = username + "@" + nodename + ":" + remoteMetaFilePath

    return source_filename

def getFile(source, destination):
    rcp_command = string.join(("rcp", source, destination), " ")
    print rcp_command
    sys.stdout.flush()

    #Get the metadata file from sdss.
    exit_status = os.spawnvp(os.P_WAIT, "rcp", ("rcp", source, destination))
    
    return exit_status

def findFile(username, nodename, dirname, tapeLabel):

    print "Warning: Incorrect mjd or tape format indicated on the " \
          "command line for volume %s.  Attepting to find par file.  " \
          " This will take a while." % (tapeLabel,)
    
    try:
        #Build the TarTape "par" filename.
        metaTarTapeFileName = "id" + "TarTape" + "-" + tapeLabel + ".par"
        #Build the TapeLog "par" filename.
        metaTapeLogFileName = "id" + "TapeLog" + "-" + tapeLabel + ".par"
        #Build the PtTape "tapelog" filename.
        metaPtTapeFileName = tapeLabel + ".tapelog"

        #Create the strings for finding the par file.
        # The find_command string is the command to search all of SDSS's
        # par files for the one describing the current tape.
        find_command = "find %s -follow -maxdepth 2 " \
                       "-name %s -o -name %s -o -name %s" % \
                       (dirname, metaTarTapeFileName,
                        metaTapeLogFileName,
                        metaPtTapeFileName)
        # The rsh_command string needs the -n option to redirect stdin
        # correctly to keep the job from being stopped in some cases.  The
        # -l <username> options tell the process to rsh as the specified user.
        rsh_command = 'rsh -n -l %s %s %s 2> /dev/null' % \
                      (username, nodename, find_command)

        #Print the current step that is about to occur.
        print rsh_command
        sys.stdout.flush()

        #Get the response from the rsh(1) of the find(1).
        pipe = os.popen(rsh_command)
        data = pipe.readlines()
        pipe.close()

        if len(data) == 1:
            #There had better be only one match.
            #Return the full remote pathname.
            return username + "@" + nodename + ":" + data[0][:-1]
        elif len(data) > 1:
            print "Multiple matches found.  Exiting."
            return None
        else:
            print "Zero matches found.  Exiting."
            return None
        
        return None
    except:
        msg = sys.exc_info()[1]
        print "Unable to find file: %s" % str(msg)
        return None

