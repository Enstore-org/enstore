#!/usr/bin/env python
"""
A daily cron job that runs a little while after midnight
"""
from __future__ import print_function

import option
import configuration_client
import time
import pg

intf = option.Interface()
csc = configuration_client.ConfigurationClient(
    (intf.config_host, intf.config_port))

acc = csc.get('accounting_server')

db = pg.DB(host=acc['dbhost'], dbname=acc['dbname'], port=acc['dbport'])

print(db.query("select * from make_daily_xfer_size();"))
print(db.query("select * from make_daily_xfer_size_by_mover();"))

day = time.localtime(time.time())[2]
if day == 1:  # beginning of the month
    print(db.query("select * from make_monthly_xfer_size();"))
