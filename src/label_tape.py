#!/usr/bin/env python
# $Id$
import sys
import string
import ftt_driver
import ftt
import e_errors

class LabelTape:
    def __init__(self, device):
        self.tape_driver = ftt_driver.FTTDriver()
        self.device = device
        
    def init_tape(self):
        raw_input("Please mount a tape and press Enter")
        first_time = 1
        have_tape = self.tape_driver.open(self.device, mode=1, retry_count=5)
        if not have_tape:
            print "No tape is mounted"
            if first_time:
                print "Please mount a tape"
            else:
                sys.exit(-1)
        stats = self.tape_driver.ftt.get_stats()
        write_prot = stats[ftt.WRITE_PROT]
        if type(write_prot) is type(''):
            write_prot = string.atoi(write_prot)
        if write_prot:
            print "Tape is write protected"
            sys.exit(-1)
        
    def tape_labeled(self):
        rc = None
        status = self.tape_driver.verify_label(None)
        if status[0]==e_errors.OK:
            rc = status[1]
        self.tape_driver.rewind()
        return rc

    def label_tape(self, label):
        vol1_label = 'VOL1'+ label
        vol1_label = vol1_label+ (79-len(vol1_label))*' ' + '0'
        self.tape_driver.write(vol1_label, 0, 80)
        self.tape_driver.writefm()
        
print "LABEL TAPE"
drive_name = raw_input("Enter drive name (can be found in /dev/rmt): ")
TapeLB=LabelTape(drive_name)
if not TapeLB:
    sys.exit(-1)

while 1:
    TapeLB.init_tape()
    TapeLB.tape_driver.rewind()
    cur_label = TapeLB.tape_labeled()
    TapeLB.tape_driver.rewind()
    if cur_label:
        print "Volume is already labeled: %s"%(cur_label,)
        reply = raw_input("Are you sure you want to relabel it? [y/n]")
        if not 'y' in reply:
            sys.exit(-1)
    reenter_cnt=0
    while reenter_cnt < 5:
        new_label = raw_input("Enter a 6 character label (exactly 6 characters) and press Enter: ")
        if reenter_cnt < 5:
            reenter_cnt = reenter_cnt + 1
            if len(new_label) == 6:
                break
    else:
       sys.exit(-1)
    
    print "rewind"
    TapeLB.tape_driver.rewind()
    print "write label"
    TapeLB.label_tape(new_label)
    print "rewind"
    TapeLB.tape_driver.rewind()
    print "check label"
    label = TapeLB.tape_labeled()
    if label != new_label:
        "wrong label is written:%s. Must be:%s"%(label, new_label)
        sys.exit(-1)
    TapeLB.tape_driver.rewind()
    TapeLB.tape_driver.eject()
    reply = raw_input("Tape is labeled as %s . Would you like another one?[y/n]"%(label,))
    if not 'y' in reply:
        sys.exit(-1)
    
    TapeLB.tape_driver.close()


