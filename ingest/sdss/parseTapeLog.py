import string

def makeTapelogFilename(tapeLabel, filemark,run,frame,ccd):
    return "idTapeLog-00" + run + "-" + ccd + "-000" + frame + ".fit"
    
def makeTarlogFilename(tapeLabel, filemark,contents,id):
    return string.lower(contents) + "." + id + ".tar"

def parseFile(filename, tapeLabel):
    filelist = []
    f = open(filename)
    line = f.readline()
    while line:
        line = string.strip(line)
        if not string.find(line, "tapelog"):
            (tape,filemark,run,frame,ccd) = string.split(line)
            filelist.append((filemark, 
                makeTapelogFilename(tapeLabel,filemark, run,frame,ccd)))
        if not string.find(line, "TARFILE"):
            (tape,filemark,contents,id) = string.split(line)
            filelist.append((int(filemark)+1, makeTarlogFilename(tapeLabel, filemark, contents, id)))
        line = f.readline()

    return filelist
    


