######################################################################
#  $Id$
#
#  dummy for aml2 so that the binary for the media changer can be built
#  without ACI library
#
######################################################################

def convert_status(int_status):
    pass


def view(volume, media_type):
    pass


def list_volser():
    pass


def drive_state(drive, client=""):
    pass


def drives_states():
    pass


def drive_volume(drive):
    pass


def mount(volume, drive, media_type, view_first=1):
    pass

# this is a forced dismount. get rid of whatever has been ejected from the
# drive


def dismount(volume, drive, media_type, view_first=1):
    pass

# home robot arm


def robotHome(arm):
    pass

# get status of robot


def robotStatus(arm):
    pass

# start robot arm


def robotStart(arm):
    pass

# home and start robot arm


def robotHomeAndRestart(ticket, classTicket):
    pass

# sift through a list of lists


def yankList(listOfLists, listPosition, look4String):
    pass


def insert(ticket, classTicket):
    pass


def eject(ticket, classTicket):
    pass


def list_slots():
    pass
