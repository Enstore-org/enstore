import string
import sys

#def makeTapelogFilename(tapeLabel, filemark, run, frame, ccd):
def makeTapelogFilename(run, frame, ccd):
    return "idTapeLog-%06d-%02d-%04d.fit"%(run, ccd, frame)
    #return "idTapeLog-00" + run + "-" + ccd + "-000" + frame + ".fit"
    
#def makeTarlogFilename(tapeLabel, filemark, contents, id):
def makeTarlogFilename(contents, id):
    return string.lower(contents) + "." + id + ".tar"

def parseFile(filename):   #, tapeLabel):
    print "FILE",filename
    filelist = []
    f = open(filename)
    line = f.readline()
    while line:
        if not line.find("tapelog"):
            #Split tapelog lines contain tuples of the following:
            # (tape, filemark, run, frame, ccd)
            (unused, filemark, run, frame, ccd) = line.split()
            filelist.append((filemark,
                             makeTapelogFilename(run, frame, ccd)))
        elif not line.find("tarfile"):
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
    


