#!/usr/bin/env python
#
# p1 -- file produces by the Trace utility.
#
#  Parse the output of TRACE to and format the HEX into english. 
#       timeStamp       PID     TIDorName         message  
#---------------------------------------------------------------------------
#       1883070939451     0      KERNEL           Cmd: 000000340000
#       1883070938804     0      KERNEL           Entered CCB #131009 Trg 6
#       1879000342269  3538      KERNEL           Cmd: 000000340000
#       1879000341904  3538      KERNEL           Entered CCB #131008 Trg 6
#       1879000317442     0      KERNEL           Cmd: 0042004d0000
#
# 

import regex
import sys

#
#  Utilities
#
# Unpack a string of 16 hex digits into a list of 8 pairs of hex digits
# while swapping to account for endianness
def list_of_eight(nbr) :
	return (nbr[6:8], nbr[4:6], nbr[2:4], nbr[0:2], 
		nbr[14:16], nbr[12:14], nbr[10:12], nbr[8:10])

# unpack a string of 12 hex digits into a list of 5 pairs of hex digits
# while swapping to account for endianness
def list_of_six(nbr) :
	return (nbr[6:8], nbr[4:6], nbr[2:4], 
		nbr[0:2], nbr[10:12], nbr[8:10])

# 
# convert a list of ASCII hex digits to a list of integer
#
def numeric_list(nbr) :
	outlist = []
	for n in nbr :
		outlist.append(eval("0x"+n))
	return outlist


########################################################################
# table driven command printing -- print the command in ascii and 
# the bytes in HEX at laest -- or provide an elaborate command specific
# subroutine to format the command.
########################################################################


cmd_txt={} #forward declaration for the linter.

# print_nothing()
def print_nothing(hexlist) : 
	hexlist = hexlist # this will quiet the linter.
	return


def print_cmd(hexlist) :
	try:
		(name, details) = cmd_txt[hexlist[0]]
	except:
		name = hexlist[0]
		details=print_nothing

	print name, hexlist
	details(hexlist)



# print details of WRITE FILEMARKS
def print_write_filemarks(hexlist) :
	numlist = numeric_list(hexlist)
	l1 = numlist[1]
	immed = ["wait-for-done(no-immed)", "return-at-begin(immed)"][l1 & 1]
        WSmk =  ["no-WSmk", "WSmk"][(l1 & 2) >>1]
	reserved = "0x%x" % ((l1 & 4+8+16) >> 2)
	lun = l1 >> 5
	print " %s %s reserved=%s lun=%s" % (immed, WSmk, reserved, lun)
	nfm = numlist[2]*256*256 + numlist[3]*256 + numlist[4]
	print " nmark=%d" % (nfm)
	return

# give a details of INQUIRY
def print_inquiry(hexlist) :
	numlist = numeric_list(hexlist)
	l1 = numlist[1]
	evpd = ["no-evpd", "evpd **ILLEGAL**"][l1 & 1]
	reserved = "0x%x" % ((l1 & 2+4+8+16) >> 2)
	lun = "0x%x" % (l1 >> 5)
	print " %s reserved=%s lun=%s" % (evpd, reserved, lun)
	page_code = numlist[2]
	print " page_code=0x%x" % (page_code)
	allocation_length = numlist[4]
	print " allocation_length=0x%x" % (allocation_length)
	link = ["no-link", "link"][numlist[5]&1]
	flag = ["no-flag", "flag"][(numlist[5]&2)>>1]
	print " link=%s flag=%s" % (link, flag)
	return  


# give a string detailing LOG SENSE
def print_logsense(hexlist) :
	numlist = numeric_list(hexlist)
	page_code = numlist[2] & (1+2+4+8+16+32)
	pc = ["Current-Threshold-Values",
	      "Current-Cumulative-Values",
	      "Default-Threshold-Values",
	      "Default-Cumulative-Values"] [numlist[2] >> 6]
	print" page_code=0x%x %s" % (page_code, pc)
	return

# give a string detailing MODE SENSE
def print_modesense(hexlist) :
	numlist = numeric_list(hexlist)
	page_code = numlist[2] & (1+2+4+8+16+32)
	pcf = ["Current-Values",
	      "Changeable-Values",
	      "Default-Values",
	      "Saved-Values"] [numlist[2] >> 6]
	print " page_code=%d %s" % (page_code, pcf)
	allocation_length = "0x%x" % (numlist[4])
	print " allocation_length=%s" % (allocation_length)



cmd_txt = {
'19' : ('erase', print_nothing), 
'12' : ('inquiry', print_inquiry),
'1b' : ('load/unload', print_nothing),
'2b' : ('locate', print_nothing),
'4c' : ('log select', print_nothing),
'4d' : ('log sense', print_logsense),
'15' : ('mode select', print_nothing),
'1a' : ('mode sense', print_modesense),
'1e' : ('prevent allow medium removal', print_nothing),
'08' : ('read', print_nothing),
'05' : ('read block limits', print_nothing),
'3c' : ('read buffer', print_nothing),
'34' : ('read position', print_nothing),
'1c' : ('recieve diagnostic results', print_nothing),
'17' : ('release unit', print_nothing),
'02' : ('request block address', print_nothing),
'03' : ('request sense', print_nothing),
'16' : ('reserve unit', print_nothing),
'01' : ('rewind', print_nothing),
'0c' : ('seek block', print_nothing),
'1d' : ('send diagnostic', print_nothing),
'11' : ('space', print_nothing),
'00' : ('test unit ready', print_nothing),
'13' : ('verify', print_nothing),
'0a' : ('write', print_nothing),
'3b' : ('write buffer', print_nothing),
'10' : ('write filemarks', print_write_filemarks)
}

###################################
# Print sensea and senseb data
###################################

sense_key_dict = {
0x00 : 'NO-SENSE',
0x01 : 'RECOVERED-ERROR',
0x02 : 'NOT-READY',
0x03 : 'MEDIUM-ERROR',
0x04 : 'HARDWARE-ERROR',
0x05 : 'ILLEGAL-REQUEST',
0x06 : 'UNIT-ATTENTION',
0x07 : 'DATA-PROTECT',
0x08 : 'BLANK-CHECK',
0x09 : 'VENDOR-SPECIFIC',
0x0a : 'COPY-ABORTED',
0x0b : 'ABORTED-COMMAND',
0x0c : 'EQUAL',
0x0d : 'VOLUME-OVERFLOW',
0x0e : 'MISCOMPARE',
0x0f : 'RESERVED'
}

additional_code_dict = {
(0x00, 0x00) : 'NO ADDITIONAL SENSE INFORMATION',
(0x00, 0x01) : 'FILEMARK DETECTED',
(0x00, 0x02) : 'END-OF-PARTITION/MEDIUM-DETECTED',
(0x00, 0x03) : 'SETMARK-DETECTED',
(0x00, 0x04) : 'BEGINNING_OF_PARITION/MEDIUM-DETECTED',
(0x00, 0x05) : 'END-OF-DATA DETECTED',
(0x0a, 0x00) : 'ERROR-LOG_OVERFLOX',
(0x80, 0x00) : 'CLEANING_REQUEST (SONY UNIQUE)',
(0x04, 0x00) : 'LOGICAL-UNIT-NOT-READY',
(0x04, 0x01) : 'LOGICAL_UNIT-IS-IN-PROCESS-OF-BECOMING-READY',
(0x3a, 0x00) : 'MEDIUM-NOT-PRESENT',
(0x00, 0x02) : 'END-OF-PARTITION/MEDIUM DETECTED',
(0x0c, 0x00) : 'WRITE-ERROR',
(0x11, 0x08) : 'UNRECOVERED-READ-ERROR',
(0x14, 0x03) : 'END-OF-DATA-NOT-FOUND',
(0x14, 0x04) : 'BLOCK-SEQUENCE-ERROR',
(0x15, 0x02) : 'POSITIONING-ERROR-DETECTED-BY-READ-OF-MEDIUM',
(0x30, 0x00) : 'INCOMPATIBLE-MEDIUM-INSTALLED',
(0x30, 0x02) : 'CANNOT-READ-MEDIUM-INCOMPATIBLE-FORMAT',
(0x31, 0x00) : 'MEDIUM-FORMAT-CORRUPTED',
(0x33, 0x00) : 'TAPE-LENGHT-ERROR',
(0x3b, 0x00) : 'SEQUENTIAL-POSITIONING-ERROR',
(0x3b, 0x01) : 'TAPE_POSITION-ERROR-AT-BEGINNING-OF-MEDIUM',
(0x3b, 0x08) : 'REPOSITION-ERROR',
(0x50, 0x00) : 'WRITE_APPEND_ERROR',
(0x71, 0x00) : 'DECOMPRESSION EXCEPTION',
(0x03, 0x00) : 'PERIHERAL-DEVICE-WRITE-FAULT',
(0x09, 0x00) : 'TRACK-FOLLOWING-ERROR',
(0x15, 0x01) : 'MECHANICAL-POSITIONING-ERROR',
(0x44, 0x00) : 'INTERNAL-TARGET-FAILURE',
(0x52, 0x00) : 'CARTRIDGE-FAULT',
(0x53, 0x00) : 'MEDIA-LOAD-OR-EJECT-FAILED',
(0x1a, 0x00) : 'PARAMETER-LIST-LENGTH-ERROR',
(0x20, 0x00) : 'INVALID-COMMAND-OPERATION-CODE',
(0x24, 0x00) : 'INVALID-FIELD-IN-CDB',
(0x25, 0x00) : 'LOGICAL-UNIT-NOT-SUPPORTED',
(0x26, 0x00) : 'INVALID-FIELD-IN-PARAMETER-LIST',
(0x26, 0x01) : 'PARAMETER-NOT-SUPPORTED',
(0x2c, 0x00) : 'COMMAND-SEQUENCE-ERROR',
(0x3d, 0x00) : 'INVALID-BITS-IN-IDENTIFY-MESSAGE',
(0x28, 0x00) : 'NOT-READY-TO-READY-TRANSITION,MEDIUM-MAY-HAVE-CHANGED',
(0x29, 0x00) : 'POWER-ON,RESET,OR-BUS-DEVICE-RESET-OCCURRED',
(0x29, 0x80) : 'DRIVE-FILED-POWER-ON-TEST(SONY UNIQUE)',
(0x2a, 0x00) : 'PARAMETERS-CHANGED',
(0x2a, 0x01) : 'MORE-PARAMETERS-CHANGED',
(0x27, 0x00) : 'WRITE-PROTECTED',
(0x00, 0x00) : 'NO-ADDIIONAL-SENSE-INFORMATION',
(0x00, 0x05) : 'END-OF-DATA-DETECTED',
(0x2C, 0x00) : 'COMMAND-SEQUENCE-ERROR',
(0x43, 0x00) : 'MESSAGE-ERROR',
(0x45, 0x00) : 'SELECT-OR-RESELECT-ERROR',
(0x47, 0x00) : 'SCSI-PARITY-ERROR',
(0x48, 0x00) : 'INITITAOR-DETECTED-ERROR-MESSAGE-RECIEVED',
(0x49, 0x00) : 'INVALID-MESSAGE-ERROR',
(0x4a, 0x00) : 'COMMAND-PHASE-ERROR',
(0x4b, 0x00) : 'DATA-PAHSE-ERROR',
(0x4e, 0x00) : 'OVERLAPPED-COMMANDS-ATTEMPED',
(0x00, 0x02) : 'END-OF-PARTITION/MEDIUM-DETECTED'
}


def print_sensea(hexlist):
	numlist = numeric_list(hexlist)
        valid = ["not-residual", "residual"] [numlist[0] >> 7]
	error_code = "%x" % (numlist[0] & 127)
	print " byte 0: %x %s Error-code=%s" % (
			numlist[0], valid, error_code)
	print " byte 1: segment_number=%x" % (numlist[1])
	b2 = numlist[2]
	file_mark=["no-file-mark","file-mark"] [b2>>7]
	eom=      ["no-eom", "eom"] [(b2 >> 6) & 1]
	ili=      ["no-ili", "ili"] [(b2 >> 5) & 1]
	reserved= ["", "RESERVED-IS-NOT-0"] [(b2 >> 5) & 1]
	sense_key= "sense-key=%s" % sense_key_dict[b2 & (1+2+4+8)]
	print " byte 2: %x %s %s %s %s %s" % (b2, 
			file_mark, eom, ili, reserved, sense_key)
	print " bytes 3-6: Information-bytes=0x%x 0x%x 0x%x 0x%x" % (
		numlist[3], numlist[4], numlist[5], numlist[6])
	print " byte 7: additional-length=0x%x" % (numlist[7])
	return

def print_senseb(hexlist) :
	numlist = numeric_list(hexlist)
	print " bytes 8-11: Command-specific-information=" + \
		"0x%x 0x%x 0x%x 0x%x" % (
		numlist[0], numlist[1], numlist[2], numlist[3])
	try:
		additional_code_text = additional_code_dict[
		  (numlist[4], numlist[5])]
	except:
		additional_code_text = "0x%x 0x%x" % (numlist[4], numlist[5])
	print " bytes 12-13: %s" % (additional_code_text)
	return



# Time -- trace prints out processor ticks.

basetime = [0L]
def format_time(timestring) :
	numtime = eval(timestring+ "L")
	if basetime[0] == 0L : 
		basetime[0] = numtime
	deltatime = basetime[0] - numtime
	return "%10.10f" % (deltatime / 400000000.0) # Assume 400 Mhz


# o.k., not very general, we oly read a file named "abort"...
try:
	fd = open(sys.argv[1], 'r')
except:
	print "parseit.py file-generated-by-Trace"
	sys.exit(1)

#
# Patterns we will parse.
#
#match strings having Cmd:
cmdpat = regex.compile(" *\([0-9]*\).*Cmd:.\([0-9a-f]*\)")   
#match "Sense B"
sensebpat = regex.compile(".*Sense B: \([0-9a-f]*\)")
#match "Sense A"
senseapat = regex.compile(".*Sense A: \([0-9a-f]*\)")
#match anything which seems to begin with time
unktimepat = regex.compile("      \([0-9][0-9]*\)\(.*\)")

while 1 :
	s = fd.readline()
	if s == "" : break	 
	if cmdpat.match(s) != -1 :
		time = cmdpat.group(1)
		cmd = cmdpat.group(2)
		list = list_of_six(cmd)
		print format_time(time),
		print_cmd (list)
	elif sensebpat.match(s) != -1 :
		nbr=sensebpat.group(1)
		hexlist = list_of_eight(nbr)
		print "senseb", nbr, hexlist
		print_senseb(hexlist)
	elif senseapat.match(s) != -1 :
		nbr = senseapat.group(1)
		hexlist = list_of_eight(nbr)
		print "senseA", nbr, hexlist
		print_sensea (hexlist)
	elif unktimepat.match(s) != -1 :
		tim = unktimepat.group(1)
		rest = unktimepat.group(2)
		print format_time(tim), rest
	else :
		print s,




