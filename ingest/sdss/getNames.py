import os

def getFile(username, filename, mjd, nodename):
    try:
        return 0
        return os.spawnvp(os.P_WAIT, "rcp", ("rcp", username + "@" + nodename + ":/sdss/data/golden/"+mjd+"/"+filename, "."))
    except:
        print "unable to fetch file"
        return -1

