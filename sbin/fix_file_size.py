#!/usr/bin/env python
from __future__ import print_function
import sys
import os
import stat
import pnfs


class mypnfs(pnfs.Pnfs):

    def get_file_size(self, filepath=None):

        if filepath:
            file = filepath
        else:
            file = self.filepath

        self.verify_existance()

        # Get the file system size.
        os_filesize = long(os.stat(file)[stat.ST_SIZE])
        self.os_filesize = os_filesize

        # If there is no layer 4, make sure an error occurs.
        try:
            pnfs_filesize = long(self.get_xreference()[2].strip())
        except ValueError:
            pnfs_filesize = long(-1)
            #self.file_size = os_filesize
            # return os_filesize

        # Error checking.  However first ignore large file cases.
        if os_filesize == 1 and pnfs_filesize > long(2**31) - 1:
            if not filepath:
                self.file_size = pnfs_filesize
            return long(pnfs_filesize)
        # Make sure they are the same.
        elif os_filesize != pnfs_filesize:
            print("Wrong file size")
            return long(pnfs_filesize)

        if not filepath:
            self.file_size = os_filesize
        return long(os_filesize)


if __name__ == "__main__":
    print(sys.argv[1])
    p = mypnfs(sys.argv[1])
    xref_fs = p.get_file_size()
    print("xref FS %s os FS %s" % (xref_fs, p.os_filesize))
    if (xref_fs != p.os_filesize) and p.os_filesize == 0:
        print("will fix")
        p.set_file_size(xref_fs)
        xref_fs = p.get_file_size()
        print("sizes after fix:xref FS %s os FS %s" % (xref_fs, p.os_filesize))
