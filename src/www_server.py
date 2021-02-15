###############################################################################
# src/$RCSfile$   $Revision$
#

"""
Allow access to all of the keys in the configuration file dictionary element WWW_SERVER.

"""

# the keyword used in the configuration dictionary
import errno
WWW_SERVER = "www_server"
SYSTEM_TAG = "system_tag"
SYSTEM_TAG_DEFAULT = ""
MEDIA_TAG = "media"
MEDIA_TAG_DEFAULT = {"aml/2": "aml/2 robot (TBD)"}


def get_system_tag(csc, timeout=0, retries=0):
    # get the keys that are associated with the web information
    try:
        www_server_keys = csc.get(WWW_SERVER, timeout, retries)
        tag = www_server_keys.get(SYSTEM_TAG, SYSTEM_TAG_DEFAULT)
    except errno.errorcode[errno.ETIMEDOUT]:
        tag = SYSTEM_TAG_DEFAULT
    # see if we were given a system tag to include at the top of every enstore
    # web page.
    return tag
