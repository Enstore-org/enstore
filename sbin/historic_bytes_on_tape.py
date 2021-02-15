#!/usr/bin/env python

import sys

import configuration_client
import enstore_functions2
import dbaccess

QUERY = "insert into historic_tape_bytes \
(date, storage_group, active_bytes, \
unknown_bytes, deleted_bytes, active_files, unknown_files ,deleted_files) \
select date_trunc('month',now()), \
storage_group, \
coalesce(sum(active_bytes),0), \
coalesce(sum(unknown_bytes),0), \
coalesce(sum(deleted_bytes),0), \
coalesce(sum(active_files),0), \
coalesce(sum(unknown_files),0), \
coalesce(sum(deleted_files),0) \
from volume where system_inhibit_0!='DELETED' and \
media_type not in ('null','disk') and library not \
like '%shelf%' and library not like '%test%' group by storage_group"

if __name__ == "__main__":
    csc = configuration_client.ConfigurationClient((enstore_functions2.default_host(),
                                                    enstore_functions2.default_port()))
    dbInfo = csc.get("database")
    db = None
    try:
        db = dbaccess.DatabaseAccess(maxconnections=1,
                                     host=dbInfo.get('db_host', "localhost"),
                                     database=dbInfo.get(
                                         'dbname', "enstoredb"),
                                     port=dbInfo.get('db_port', 5432),
                                     user=dbInfo.get('dbuser', "enstore"))
        db.insert(QUERY)
    except Exception as e:
        sys.stderr.write(str(e))
        sys.stderr.flush()
        sys.exit(1)
    finally:
        if db:
            db.close()
