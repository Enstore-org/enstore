import os

def getFile(username, filename, mjd, nodename):
    try:
        return os.spawnvp(os.P_WAIT, "rcp", ("rcp", username + "@" + nodename + ":/sdss/data/golden/"+mjd+"/"+filename, "."))
    except:
        print "unable to fetch file"

