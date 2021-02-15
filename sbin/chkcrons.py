#!/usr/bin/env python

from __future__ import print_function
import ecron_util
import os

edata = ecron_util.EcronData()

# check if ecrons_dir exist
if not os.access(edata.crons_dir, os.F_OK):
    os.makedirs(edata.crons_dir)

names_and_nodes = edata.get_all_names_and_nodes()
for i in names_and_nodes:
    if i[1].split(".")[0] in edata.monitored_nodes:
        print(i[0], i[1])
        dir = os.path.join(edata.crons_dir, i[1].split('.')[0])
        # check if the directory exists
        if not os.access(dir, os.F_OK):
            os.makedirs(dir)
        start = None
        res = edata.get_result(i[0], i[1], ecron_util.one_week_ago())
        # gid more into history if it is necessary
        if res == []:
            res = edata.get_result(i[0], i[1], ecron_util.one_month_ago())
            start = ecron_util.one_month_ago()
            if res == []:
                res = edata.get_result(i[0], i[1], ecron_util.six_month_ago())
                start = ecron_util.six_month_ago()
        edata.plot(os.path.join(dir, i[0]), i[0], res, start)
