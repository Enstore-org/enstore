import e_errors
status=('READ_VOL1_READ_ERR', 'FTT_EBLANK', ('a read system call: doing ftt_read on /dev/rmt/tps11d0n returned -1,\n\terrno 5, => result -1, ftt error FTT_EBLANK(12), meaning \n\tthat we encountered blank tape or end of tape.\n', 12))
BLANK_RETURN_PATTERN1 = 'a read system call: doing ftt_read'
BLANK_RETURN_PATTERN2 = 'returned -1,\n\terrno 5, => result -1, ftt error FTT_EBLANK(12), meaning \n\tthat we encountered blank tape or end of tape.\n'

if status[0] == e_errors.READ_VOL1_READ_ERR:
# this can be the blank tape
    if status[1] == 'FTT_EBLANK':
        print "A"
        if (len(status) > 2 
            and blank_return_pattern1 in status[2][0]
            and blank_return_pattern2 in status[2][0]):
            print "B"
            
