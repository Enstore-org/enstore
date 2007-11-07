#!/usr/bin/env python

###############################################################################
# $Id$
#
# return  hostname using aliases
#
#
#

import os
import validate_host

if __name__ == '__main__':
    if validate_host.is_on_host(os.environ['ENSTORE_CONFIG_HOST'], use_alias=1):
        print os.environ['ENSTORE_CONFIG_HOST']

