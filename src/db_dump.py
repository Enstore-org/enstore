#
# db_dump.py -- dump database
#

import restore
import sys

d = restore.DbTable(sys.argv[1], [])
d.dump()
