import os
import sys
import string

#Get the file contianing the metadata for the tapetype/volume filename.
GOLDEN_DIR = "/sdss/data/golden/"

def getFile(username, nodename, filename, metadata_output):

    #Create the full remote file path to the sdss catalog metadata.
    source_filename = username + "@" + nodename + ":" + filename

    rcp_command = string.join(("rcp", source_filename, metadata_output), " ")
    print rcp_command
    sys.stdout.flush()

    #Get the metadata file from sdss.
    exit_status = os.spawnvp(os.P_WAIT, "rcp",
                             ("rcp", source_filename, metadata_output))
    
    return exit_status

def findFile(username, nodename, mjd, tapeLabel):
    try:
        #Build the TarTape "par" filename.
        metaTarTapeFileName = "id" + "TarTape" + "-" + tapeLabel + ".par"
        #Set the fullpath of this file.
        remoteMetaTarTapeFilePath = os.path.join(GOLDEN_DIR, mjd,
                                                 metaTarTapeFileName)
        
        #Build the TapeLog "par" filename.
        metaTapeLogFileName = "id" + "TapeLog" + "-" + tapeLabel + ".par"
        #Set the fullpath of this file.
        remoteMetaTapeLogFilePath = os.path.join(GOLDEN_DIR, mjd,
                                                 metaTapeLogFileName)

        ls_command = "ls %s %s" % (remoteMetaTarTapeFilePath,
                                   remoteMetaTapeLogFilePath)
        rsh_command = 'rsh -l %s %s %s 2> /dev/null' % \
                      (username, nodename, ls_command)

        print rsh_command
        sys.stdout.flush()
        
        pipe = os.popen(rsh_command)
        data = pipe.readlines()
        pipe.close()

        if data:
            #There had better be only one match.
            return data[0][:-1]

        print "Warning: Incorrect mjd or tape format indicated on the " \
              "command line for volume %s.  Attepting to find par file.  " \
              " This will take a while." % (tapeLabel,)

        find_command = "find %s -follow -maxdepth 2 " \
                       "-name %s -o -name %s" % \
                       (GOLDEN_DIR, metaTarTapeFileName,
                        metaTapeLogFileName)
        rsh_command = 'rsh -l %s %s %s 2> /dev/null' % \
                      (username, nodename, find_command)

        print rsh_command
        sys.stdout.flush()
        
        pipe = os.popen(rsh_command)
        data = pipe.readlines()
        pipe.close()

        if data:
            #There had better be only one match.
            return data[0][:-1]
        
        return None
    except:
        msg = sys.exc_info()[1]
        print "Unable to find file: %s" % str(msg)
        return None

