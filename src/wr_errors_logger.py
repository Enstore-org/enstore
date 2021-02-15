#!/usr/bin/env python
from __future__ import print_function
import sys
import os

if len(sys.argv) != 2:
    print("Usage %s enstore-log-file-name" % (sys.argv[0],))
    sys.exit(1)
infile = sys.argv[1]
outfile = "/tmp/DSW-%s" % (infile.split("G-")[1],)
print(outfile)

wr_stats = {}
# create output file
os.system('grep "drive stats after write" %s > %s' % (infile, outfile))
of = open(outfile, "r")

while True:
    l = of.readline()
    if l:
        a = l.split(" ")
        # expected line format:
        # 23:59:48 stkenmvr205a.fnal.gov 006277 root I LTO4_105MV  drive stats
        # after write. Tape VOY318 position 180 block 0 block_size 0 bloc_loc
        # 2835120 tot_blocks 762939000 BOT 0 read_err 0 write_err 0 bytes
        # 2119911936 block_write_tot 19.2541582584 tape_rate 110101511.972
        # Thread tape_thread
        t = a[0]
        mv = a[5]
        vol = a[12]
        pos = int(a[14])
        read_err = long(a[26])
        write_err = long(a[28])
        bytes_written = long(a[30])
        write_time = float(a[32])
        tape_rate = float(a[34])
        tp = (
            t,
            vol,
            pos,
            read_err,
            write_err,
            bytes_written,
            write_time,
            tape_rate)
        if not (mv in wr_stats):
            wr_stats[mv] = {"write_count": 0}
        if not (vol in wr_stats[mv]):
            wr_stats[mv][vol] = []
        wr_stats[mv][vol].append(tp)
        wr_stats[mv]["write_count"] = wr_stats[mv]["write_count"] + 1
    else:
        break

for mv in wr_stats.keys():
    mv_writes = wr_stats[mv]["write_count"]
    mv_wr_errors = 0
    print("Mover", mv)
    print("Total writes", mv_writes)
    for vol in wr_stats[mv].keys():
        if vol != "write_count":
            vol_writes = len(wr_stats[mv][vol])
            vol_wr_errors = 0
            for i in wr_stats[mv][vol]:
                vol_wr_errors = vol_wr_errors + i[4]
            mv_wr_errors = mv_wr_errors + vol_wr_errors
            print(
                "Vol. %s writes %s write_errors %s" %
                (vol, vol_writes, vol_wr_errors))
    print("Total write errors", mv_wr_errors)
    print("============================================")
