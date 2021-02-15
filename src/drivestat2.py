#!/usr/bin/env python

import pg
import os
import pprint
import time

# time2timestamp(t) -- convert time to "YYYY-MM-DD HH:MM:SS"


def time2timestamp(t):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t))

# timestamp2time(ts) -- convert "YYYY-MM-DD HH:MM:SS" to time


def timestamp2time(s):
    return time.mktime(time.strptime(s, "%Y-%m-%d %H:%M:%S"))


class dsDB:
    def __init__(self, host, dbname, user, port=None, logname='UNKNOWN'):
        self.logname = logname
        if port:
            self.db = pg.DB(host=host, dbname=dbname, user=user, port=port)
        else:
            self.db = pg.DB(host=host, dbname=dbname, user=user)
        self.pid = os.getpid()

    def close(self):
        self.db.close()

    def insert(self, table, dict):
        return self.db.insert(table, dict)

    def log_stat(self,
                 drive_sn,
                 drive_vendor,
                 product_type,
                 firmware_version,
                 host,
                 logical_drive_name,
                 stat_type,
                 time2,
                 tape_volser,
                 power_hrs,
                 motion_hrs,
                 cleaning_bit,
                 mb_user_read,
                 mb_user_write,
                 mb_dev_read,
                 mb_dev_write,
                 read_errors,
                 write_errors,
                 track_retries,
                 underrun,
                 mount_count,
                 wp=0,
                 mover_name=None):

        if not isinstance(time2, type("")):
            time2 = time2timestamp(time2)

        values = {
            "drive_sn": drive_sn,
            "drive_vendor": drive_vendor,
            "product_type": product_type,
            "firmware_version": firmware_version,
            "host": host,
            "logical_drive_name": logical_drive_name,
            "stat_type": stat_type,
            "time": time2,
            "tape_volser": tape_volser,
            "power_hrs": power_hrs,
            "motion_hrs": motion_hrs,
            "cleaning_bit": cleaning_bit,
            "mb_user_read": mb_user_read,
            "mb_user_write": mb_user_write,
            "mb_dev_read": mb_dev_read,
            "mb_dev_write": mb_dev_write,
            "read_errors": read_errors,
            "write_errors": write_errors,
            "track_retries": track_retries,
            "underrun": underrun,
            "mount_count": mount_count,
            "wp": wp,
            "mover_name": mover_name
        }

        self.insert('status', values)
