###############################################################################
# $Id$


# create header
def create_header(inode, mode, uid, gid, nlink, mtime, filesize,
             major, minor, rmajor, rminor, filename):

    return ""

# generate the enstore cpio "trailers"
def trailers(blocksize, siz, trailer):
    return ""
             
class Wrapper :

    def sw_mount( self, driver, info ):
        ##print "NULL sw_mount"
	return

    # generate an enstore cpio archive: devices must be open and ready
    def write_pre_data( self, driver, info ):
        ##print "NULL write-pre-data"
        return

    def write_post_data( self, driver, crc ):
        ##print "NULL write-post-data"
        return

    def read_pre_data( self, driver, info ):
        ##print "NULL read-pre-data"
        return

    def read_post_data( self, driver, info ):
        ##print "NULL read-post-data"
        return
