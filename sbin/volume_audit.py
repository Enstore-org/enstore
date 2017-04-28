#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

"""
This script select a random tape, takes first, last and random file in between
and performs encp of these files.
"""

from optparse import OptionParser
import os
import random
import string
import sys
import time


import configuration_client
import dbaccess
import encp_wrapper
import enstore_functions2
import e_errors
import library_manager_client
import mover_client
import Trace

QUERY="""
SELECT f.bfid,
       f.location_cookie,
       f.volume,
       v.label
FROM file f,
     volume v
WHERE f.volume=v.id
   AND f.volume IN
      (SELECT id
       FROM volume
       WHERE library IN ('{}')
          AND system_inhibit_0='none'
          AND (system_inhibit_1 IN ('readonly','full')
               OR last_access < CURRENT_TIMESTAMP - interval '30 days')
          AND media_type NOT IN ('disk','null')
          AND label NOT LIKE 'CLN%'
          AND active_files>0
          AND id NOT IN
             (SELECT DISTINCT volume
              FROM volume_audit
              WHERE finish>CURRENT_TIMESTAMP-interval '{} days')
       ORDER BY random() LIMIT 1)
   AND f.deleted='n'
   AND f.size>0
ORDER BY f.location_cookie ASC
"""
"""SQL query that produces a list of bfid,location_cookie,volume from a randomly
selected volume. Query is postgresql specific"""

INSERT_QUERY_WITH_ERROR="""
INSERT INTO volume_audit (volume, start,finish, bfid,result,error)
VALUES ({},'{}',{},'{}',{},'{}')
"""
"""SQL query that insert a record containing and error string into volume_audit table"""

INSERT_QUERY_WO_ERROR="""
INSERT INTO volume_audit (volume, start,finish, bfid,result)
VALUES ({},'{}',{},'{}',{})
"""
"""SQL query that insert a record into volume_audit table w/o error string"""

ENCP_ARGS=["encp"] + ["--skip-pnfs",
                      "--verbose", "10",
                      "--priority", "10",
                      "--bypass-filesystem-max-filesize-check",
                      "--max-resubmit", "7",
                      "--delayed-dismount", "1",
                      "--get-bfid"]
"""encp options"""

Trace.init("VOLUME_AUDIT")


def help():
    return "usage %prog [options]"


class VolumeAudit:
    """Performs volume audit by randomly choosing a tape and running
    encp of first, last and random file in between from that tape. Records
    encp result in enstoredb"""
    def __init__(self,csc,duration=360):
        """constructor.

        :type csc: :class:`configuration_client.ConfigurationClient`
        :arg csc: configuration client.
        :type duration: integer
        :arg duration: how often to audit a tape (e.g. once in 360 days)
        """
        self.csc = csc
        self.duration=duration
        dbInfo   = csc.get("database")
        self.db  = dbaccess.DatabaseAccess(maxconnections=1,
                                           host     = dbInfo.get('db_host', "localhost"),
                                           database = dbInfo.get('dbname', "enstoredb"),
                                           port     = dbInfo.get('db_port', 5432),
                                           user     = dbInfo.get('dbuser', "enstore"))

    def do_work(self):
        """Performs volume audit by randomly choosing a tape and running
        encp of first, last and random file in between from that tape. Records
        encp result in enstoredb"""
        #
        # get list of library managers and build list of libraries
        # that have sufficient number of movers
        #
        lms = self.csc.get_library_managers2()
        libs = []
        """
        exclude test tape libraries
        """
        test_libraries = self.csc.get('crons',{}).get('test_library_list', [])
        lms = [lm for lm in lms if lm['library_manager'] not in test_libraries]
        for l in lms:
            lmc = library_manager_client.LibraryManagerClient(self.csc, l["name"])
            active_volumes = lmc.get_active_volumes()
            movers = self.csc.get_movers2(l["name"])
            movers = [ x.get('name') for x in movers
                       if x.get('status') == (e_errors.OK, None) and x.get('name') ]
            hasIdle=False
            for m in movers:
                mvc=mover_client.MoverClient((enstore_functions2.default_host(),
                                              enstore_functions2.default_port()), m)
                m_status = mvc.status()
                if m_status.get('state') in ('IDLE',):
                    hasIdle=True
                    break
            if hasIdle:
                libs.append(l["name"].split(".")[0])
        if len(libs) == 0 :
            Trace.log(e_errors.INFO, "All libraries are too busy")
            return
        #
        # inject libraries and duration into the SQL.
        # There is no guarantee that the movers belonging to
        # a volume library will not be all busy by the
        # time the query finishes. Not sure what we can
        # do about it.
        #
        q=QUERY.format(string.join(libs,"','"),self.duration)
        res=self.db.query(q)
        l = len(res)
        if l <= 0 :
            Trace.alarm(e_errors.INFO, "Could not find a random volume. Decrease -d option value. Current value is %d"%(self.duration))
            return
        # pick random file from interval that excludes 1st and last
        index = random.randint(1,l-2) if l>2 else 0
        volume=res[0][2]
        label =res[0][3]
        random_file = res[index][0]
        first_file  = res[0][0]
        last_file   = res[l-1][0]
        bfid_set    = set([first_file,random_file,last_file])
        error_msg=None
        start=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        bfid=None
        rc=0
        for bfid in bfid_set:
            argv=ENCP_ARGS+[bfid,"/dev/null"]
            encp=encp_wrapper.Encp()
            rc        = encp.encp(argv)
            error_msg = encp.err_msg
            #
            # first breaks, quit
            #
            if rc :
                break
        if error_msg:
            #
            # need to escape single ' with '' for SQL to work
            #
            q=INSERT_QUERY_WITH_ERROR.format(volume,start,'now()',bfid,rc,error_msg.replace("'", "''"),)
            if rc:
                Trace.alarm(e_errors.WARNING, "Failed on volume %s, bfid %s with error %s, return code %d"%(label,bfid,error_msg,rc))
        else:
            q=INSERT_QUERY_WO_ERROR.format(volume,start,'now()',bfid,rc)
            if rc:
                Trace.alarm(e_errors.WARNING, "Failed on volume %s, bfid %s with return code %d"%(label,bfid,rc,))
        self.db.insert(q)
        return rc


    def __del__(self):
        """destructor"""
        self.db.close()


if __name__ == "__main__":
    parser = OptionParser(usage=help())
    parser.add_option("-d", "--days",
                      metavar="DAYS",type=int,default=360,dest="days",
                      help="select volumes that were audited no sooner than DAYS days ago (or never) [default: %default]")
    (options, args) = parser.parse_args()

    csc = configuration_client.ConfigurationClient((enstore_functions2.default_host(),
                                                    enstore_functions2.default_port()))
    audit = VolumeAudit(csc,options.days)
    rc = audit.do_work()
    sys.exit(rc)

