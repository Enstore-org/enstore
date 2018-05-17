#!/usr/bin/env python
##############################################################################
#
# This test performs mount/dismounts of set of tapes on est of drives
#
##############################################################################
import time
import random
import sys
import os
import threading

import media_changer_client
import volume_clerk_client
import e_errors
import mover_client
import enstore_constants
import enstore_functions2
import accounting_client
import Trace

STOP_FILE = '/tmp/STOP'

class MountTest():
    def __init__(self, media_changer, drives, volumes):
        self.logname = 'MOUNTTEST'
        Trace.init(self.logname, 'yes')
        self.csc = (enstore_functions2.default_host(),
                    enstore_functions2.default_port())
        self.asc = accounting_client.accClient(self.csc, self.logname)
        self.mcc = media_changer_client.MediaChangerClient(self.csc, media_changer)
        self.vcc = volume_clerk_client.VolumeClerkClient(self.csc)
        self.drives = [] # [[driveN, volume_nameN],..]
        for d in drives:
            self.drives.append([d, 'empty'])
        print "S.DRIVES", self.drives
        self.volumes = []
        for v in volumes:
            self.volumes.append(v)
        print "S.VOLS", self.volumes
        self.in_work = []
        self.mounted = threading.Event()
        self.dismounted = threading.Event()

        

    def stop(self):
        if os.path.exists(STOP_FILE):
            print("Found %s file, Stopping ..."%(STOP_FILE,))
            return True
        return False
        
    def mount(self, volume, drive):
        print "Mount",volume, drive
        for d in self.drives:
            if drive == d[0] and d[1] == 'empty':
                break
        else:
            Trace.log(e_errors.INFO, "Drive %s is not empty: %s"%(d[0], d[1]))
            return

        vi = self.vcc.inquire_vol(volume)
        tm = time.localtime(time.time()) # get the local time
        time_msg = "%.2d:%.2d:%.2d" %  (tm[3], tm[4], tm[5])
        Trace.log(e_errors.INFO, "mounting %s %s"%(volume, time_msg),
                  msg_type=Trace.MSG_MC_LOAD_REQ)
        self.asc.log_start_mount(volume, vi['media_type'])

        mc_queue_full = 0
        while 1:
            mcc_reply = self.mcc.loadvol(vi, drive, drive)
            status = mcc_reply.get('status')
            if status and status[0] == e_errors.MC_QUEUE_FULL:
                # media changer responded but could not perform the operation
                Trace.log(e_errors.INFO, "Media Changer returned %s"%(status[0],))
                # to avoid false "too long in state.."
                # reset self.time_in_state
                self.time_in_state = time.time()
                time.sleep(10)
                mc_queue_full = 1
                continue
            else:
                break

        if status[0] != e_errors.OK and mc_queue_full:
            if status[0]:
                tm = time.localtime(time.time()) # get the local time
                time_msg = "%.2d:%.2d:%.2d" %  (tm[3], tm[4], tm[5])
                Trace.log(e_errors.INFO, "mounting failed for %s %s. Error %s"%(volume, time_msg, status))

        if status[0] == e_errors.OK:
            self.vcc.update_counts(volume, mounts=1)
            self.asc.log_finish_mount(volume)
            d[1] = volume
            tm = time.localtime(time.time()) # get the local time
            time_msg = "%.2d:%.2d:%.2d" %  (tm[3], tm[4], tm[5])
            Trace.log(e_errors.INFO, "mounted %s %s"%(volume, time_msg),
                      msg_type=Trace.MSG_MC_LOAD_DONE)
            self.mounted.set()
            print "mounted", volume, drive

        else: #Mount failure, do not attempt to recover
            Trace.log(e_errors.ERROR, "mount %s: %s" % (volume, status))
            self.asc.log_finish_mount_err(volume)

    def dismount(self, volume, drive=None):
        if drive:
            d = [drive, volume]
        else:
            for d in self.drives:
                if d[1] == volume:
                    break
            else:
                Trace.log(e_errors.INFO, "volume %s is not mounted"%(d[1],))
                return
        print "dismount", d

        vi = self.vcc.inquire_vol(volume)
        tm = time.localtime(time.time()) # get the local time
        time_msg = "%.2d:%.2d:%.2d" %  (tm[3], tm[4], tm[5])
        Trace.log(e_errors.INFO, "dismounting %s %s"%(volume, time_msg),
                  msg_type=Trace.MSG_MC_LOAD_REQ)
        self.asc.log_start_dismount(volume, vi['media_type'])

        vi = self.vcc.inquire_vol(volume)
        while 1:
            mcc_reply = self.mcc.unloadvol(vi, d[0], d[0])
            status = mcc_reply.get('status')
            if status and status[0] == e_errors.MC_QUEUE_FULL:
                # media changer responded but could not perform the operation
                Trace.log(e_errors.INFO, "Media Changer returned %s"%(status[0],))
                # to avoid false "too long in state.."
                # reset self.time_in_state
                self.time_in_state = time.time()
                time.sleep(10)
                continue
            else:
                break
        if status and status[0] == e_errors.OK:
            self.asc.log_finish_dismount(volume)
            tm = time.localtime(time.time())
            d[1] = 'empty'
            if volume in self.in_work:
                self.in_work.remove(volume)
            self.dismounted.set()
            time_msg = "%.2d:%.2d:%.2d" %  (tm[3], tm[4], tm[5])
            Trace.log(e_errors.INFO, "dismounted %s %s"%(volume, d[0]), time_msg)
            print "dismounted", volume, d[0]

        else:
            self.asc.log_finish_dismount_err(self.current_volume)
            Trace.log(e_errors.ERROR, "dismount %s: %s" % (self.current_volume, status))
        return

    def select_tape(self):
        while 1:
            ran = random.randrange(0, len(self.volumes), 1)
            if self.volumes[ran] in self.in_work:
                if len(self.volumes) == len(self.in_work):
                    # no available volumes left
                    return None
                else:
                    continue
            else:
                self.in_work.append(self.volumes[ran])
                break
        return self.volumes[ran] 
        
    def mount_tapes(self):
        while not self.stop():
            found = False
            while not found:
                volume = self.select_tape()
                # find empty drive
                for d in self.drives:
                    if d[1] == 'empty':
                        found = True
                        break
                else:
                    self.dismounted.wait(10)
                    self.dismounted.clear()
                    continue
            
            self.mount(volume, d[0])

    def dismount_tapes(self):
        while not self.stop():
            found = False
            while not found:
                for d in self.drives:
                    if d[1] != 'empty':
                        found = True
                        break
                else:
                    self.mounted.wait(10)
                    self.mounted.clear()
                    continue
            
            self.dismount(d[1])
        for d in self.drives: 
            if d[1] != 'empty':
                self.dismount(d[1])


if __name__ == "__main__":
    vols = []
    drives = []
    volsfd = open(sys.argv[1], 'r')
    drivesfd = open(sys.argv[2], 'r')
    while 1:
        l = volsfd.readline()
        if l:
            vols.append(l.strip())
        else:
            break
    while 1:
        l = drivesfd.readline()
        if l:
            drives.append(l.strip())
        else:
            break
    print "VOLS",vols
    print "DRIVES", drives
    TestInstance = MountTest(sys.argv[3], drives, vols)
    for d in drives:
        # dismout if mounted
        ds = TestInstance.mcc.viewdrive(drives[0])
        if ds['drive_info']['volume'] == '':
            continue
        else:
            TestInstance.dismount(ds['drive_info']['volume'], drive=d)
            TestInstance.dismounted.clear()
    mount_thread = threading.Thread(target=TestInstance.mount_tapes, name='Mount-Thread')
    dismount_thread = threading.Thread(target=TestInstance.dismount_tapes, name='Dismount-Thread')
    mount_thread.start()
    dismount_thread.start()
    print "WAITING to join"
    mount_thread.join()
    dismount_thread.join()
    print "DONE"
