#!/usr/bin/env python
import os
import sys
import errno

import configuration_client
import file_clerk_client
import alarm_client
import enstore_functions2
import dbaccess
import e_errors
import option
import Trace

MY_NAME = 'RM_EMPTY_PACK'

"""
SQL query that produces a list of bfid,pnfs_id,pnfs_path for all SFA packages with 0 active files
and which are not deleted
"""
QUERY_EMPTY_PACKAGES = """
SELECT bfid,
       pnfs_id,
       pnfs_path
FROM file
WHERE package_id=bfid
   AND package_files_count!=0
   AND active_package_files_count=0
   AND deleted='n'
   AND archive_mod_time < CURRENT_TIMESTAMP - interval '1 week'
ORDER BY bfid ASC
"""
"""
SQL query that returns the number of not deleted meberes for a given package.
The count includes the package itself
"""
QUERY_MEMBERS = """
SELECT count(*)
FROM file
WHERE package_id=%s
   AND deleted!='y'
"""


class Remover:
    def __init__(self):
        self.fcc = file_clerk_client.FileClient((enstore_functions2.default_host(),
                                                 enstore_functions2.default_port()))
        dbInfo = self.fcc.csc.get('database')
        self.db = dbaccess.DatabaseAccess(maxconnections=1,
                                          host=dbInfo.get(
                                              'db_host', "localhost"),
                                          database=dbInfo.get(
                                              'dbname', "enstoredb"),
                                          port=dbInfo.get('db_port', 5432),
                                          user=dbInfo.get('dbuser_reader', "enstore_reader"))
        self.removed = 0
        self.total = 0

    def member_files(self, package_id):
        return self.db.query(QUERY_MEMBERS, (package_id,))[0][0]

    def rm_empty_packages(self):
        res = self.db.query(QUERY_EMPTY_PACKAGES)
        if len(res) <= 0:
            Trace.log(e_errors.INFO, 'No packages with 0 active files found')
            return False
        fail = False
        self.total = len(res)
        for f in res:
            rc = self.member_files(f[0])
            if rc == 1:  # 0 members. 1- stands for the package itself
                try:
                    os.remove(f[2])
                    Trace.log(
                        e_errors.INFO, 'Removed empty package %s' %
                        (f[2],))
                    self.removed += 1
                except Exception as e:
                    fail = True
                    Trace.log(
                        e_errors.ALARM, 'Error %s removing empty package %s' %
                        ((e, f[2])))
        return fail

    def do_work(self):
        alarm_client.Trace.init(MY_NAME)
        exit_code = 0
        try:
            exit_status = self.rm_empty_packages()
        except (SystemExit, KeyboardInterrupt) as msg:
            Trace.log(e_errors.ERROR, 'delfile aborted from: %s' % str(msg))
            exit_code = 1
        except BaseException:
            Trace.handle_error()
            exit_code = 1
        if exit_status:
            Trace.alarm(
                e_errors.WARNING,
                "failed to remove at least one file, see log")
            exit_code = 1
        Trace.log(e_errors.INFO, '%d empty packages found. %s packages removed. %s packages failed to remove' %
                  (self.total, self.removed, self.total - self.removed))
        sys.exit(exit_code)


if __name__ == "__main__":
    remover = Remover()
    remover.do_work()
