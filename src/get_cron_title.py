#!/usr/bin/env python

from __future__ import print_function
import sys
import os

import enstore_make_plot_page

# get input param

if len(sys.argv) < 2:
    # there was no param entered
    os._exit(1)
else:
    cron = sys.argv[1]

# find associated text
print(enstore_make_plot_page.ENGLISH_TITLES.get(cron, ""))
