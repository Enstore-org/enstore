import string
import sys

#def makeTapelogFilename(tapeLabel, filemark, run, frame, ccd):
def makeTapelogFilename(run, frame, ccd):
    return "idTapeLog-00" + run + "-" + ccd + "-000" + frame + ".fit"
    
#def makeTarlogFilename(tapeLabel, filemark, contents, id):
def makeTarlogFilename(contents, id):
    return string.lower(contents) + "." + id + ".tar"

def parseFile(filename):   #, tapeLabel):
    filelist = []
    f = open(filename)
    line = f.readline()
    while line:
        line = string.strip(line)
        if not string.find(line, "tapelog"):
            #Split tapelog lines contain tuples of the following:
            # (tape, filemark, run, frame, ccd)
            (unused, filemark, run, frame, ccd) = string.split(line)
            filelist.append((filemark,
                             makeTapelogFilename(run, frame, ccd)))
        elif not string.find(line, "TARFILE"):
            #Split TARFILE lines contain tuples of the following:
            # (tape, filemark, contents, tar_id)
            (unused, filemark, contents, tar_id) = string.split(line)
            filelist.append((int(filemark) + 1,
                             makeTarlogFilename(contents, tar_id)))
        #else:
        #    sys.stderr.write("Invalid tape type found.\n")
        #    sys.exit(50)
            
        line = f.readline()

    f.close()  #Cleanup
    return filelist
    


