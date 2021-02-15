#!/usr/bin/env python

from __future__ import print_function
import time
import string
import sys

import option
import volume_clerk_client
import e_errors
import enstore_functions2


def usage(s):
    print('usage: %s [yyyy-mm-dd [hh:mm:ss]]' % (s))


if __name__ == '__main__':

    # help
    if '-h' in sys.argv or '--help' in sys.argv:
        usage(sys.argv[0])
        sys.exit(0)

    # processing time
    if len(sys.argv) == 1:  # no argument
        last = 0
    elif len(sys.argv) > 3:
        usage(sys.argv[0])
        sys.exit(1)
    else:
        # default values: now
        (year, month, day, hour, minute, second, weekday,
         julian_day, daylight) = time.localtime(time.time())
        hour = 0
        minute = 0
        second = 0
        try:
            for i in sys.argv[1:]:
                if string.find(i, '-') != -1:
                    date = string.split(i, '-')
                    year = int(date[0])
                    month = int(date[1])
                    day = int(date[2])
                else:
                    ttime = string.split(i, ':')
                    hour = int(ttime[0])
                    if len(ttime) > 1:
                        minute = int(ttime[1])
                        if len(ttime) > 2:
                            second = int(ttime[2])
            last = time.mktime(
                (year, month, day, hour, minute, second, 0, 0, -1))
        except BaseException:
            usage(sys.argv[0])
            sys.exit(1)

    intf = option.Interface()
    vcc = volume_clerk_client.VolumeClerkClient(
        (intf.config_host, intf.config_port))

    # get list of volume

    vols = vcc.get_vol_list()['volumes']

    for i in vols:
        if i[-8:] != '.deleted':
            vol = vcc.inquire_vol(i)
            if vol['status'][0] == e_errors.OK and \
                    vol['media_type'] != 'null' and \
                    enstore_functions2.is_readonly_state(vol['system_inhibit'][1]):
                if 'si_time' in vol:
                    t = vol['si_time'][1]
                else:
                    t = 0
                if t >= last:
                    print(
                        i,
                        vol['system_inhibit'][1],
                        vol['library'],
                        time.ctime(t))
