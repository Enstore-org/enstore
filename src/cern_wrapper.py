import time
import string
import os

import enstore_functions2

# ticket keys used in this module
BFID = 'bfid'
BLOCKLEN = 'blocksize'
CHECKSUM = 'checksum'
COMPRESSION = 'compression'
DRIVEMFG = 'vendor_id'
DRIVEMODEL = 'product_id'
DRIVESERIALNUMBER = 'serial_num'
ENCPVERSION = 'version'
EXPERIMENT = 'storage_group'
GID = 'gid'
MODE = 'mode'
MOVERNODE = 'host'
PNFSFILENAME = 'pnfsFilename'
SIZEBYTES = 'size_bytes'
UID = 'uid'
USERNAME = 'uname'

SPACE = " "
MINUS1 = -1
ZERO = "0"
ONE = "1"
RECORDFORMAT = ['F', 'D', 'S']
FIXED = 0       # index in to RECORDFORMAT
VARIABLE = 1    # index in to RECORDFORMAT
SEGMENTED = 2   # index in to RECORDFORMAT
HDR_LABELLEN = 80
FILENAMELEN1 = 17
UHLNFILECHUNK = HDR_LABELLEN - 4
MAXFILENAMELEN = (26*UHLNFILECHUNK)+FILENAMELEN1
FERMILAB = "Fermilab"  # this string must remain 8 bytes long
OWNER = "Fermilab      "  # this string must be 14 bytes long
BLOCK_LEN_LIMIT = 99999L
RECORD_LEN_LIMIT = 99999L
ADLER32 = "AD"

BFID_L = 20
BLOCK_LENGTH_L = 5
DRIVE_MFG_L = 8
DRIVE_MODEL_L = 8
DRIVE_SERIAL_NUMBER_L = 12
ENCPVERSION_L = 13
EXPERIMENT_L = 8
FILE_CHECKSUM_L = 10
FILE_SIZE_L = 20
GID_L = 10
HOSTNAME_L = 10
RECORD_LENGTH_L = 5
UID_L = 10
USERNAME_L = 14
COMPRESSION_USED = "P"
CVSREVISION = "CVS $Revision: "
efile = None 

# exceptions that this module can raise
UNKNOWNRECFORMAT = "UNKNOWN_RECORD_FORMAT"
INVALIDLENGTH = "INVALID_LENGTH"

def get_cent_digit(date_l):
    if date_l[0] < 2000:
	cent_digit = SPACE
	cent = 1900
    else:
	cent_digit = ZERO
	cent = 2000
    return cent, cent_digit

def get_date(now):
    if not now == MINUS1:
	date_l = time.gmtime(now)
	cent, cent_digit = get_cent_digit(date_l)
	# the format is cent_digit then year (2 chars), then julian day.
	# make the julian day be 3 chars long with preceeding 0's
	return "%s%s%s"%(cent_digit, 
			 string.replace("%2s"%(date_l[0] - cent,), " ", "0"),
			 string.replace("%3s"%(date_l[7],), " ", "0"))
    else:
	# this format means no time was specified
	cent, cent_digit = get_cent_digit(time.gmtime(time.time()))
	return "%s%s"%(cent_digit, 5*ZERO)


def add_r_padding(aString, maxLength, padChar=SPACE):
    l = len(aString)
    if l < maxLength:
	p = maxLength - l
	aString = "%s%s"%(aString, p*padChar)
    return aString

def add_l_padding(aString, maxLength, padChar=SPACE):
    l = len(aString)
    if l < maxLength:
	p = maxLength - l
	aString = "%s%s"%(p*padChar, aString)
    return aString

class Label:

    def __init__(self):
	self.label = 4*SPACE
	self.text = ""

    def text_len(self):
	if not len(self.text) == HDR_LABELLEN:
	    # we need to be of length HDR_LABELLEN
	    raise INVALIDLENGTH, \
		  "invalid length (%s) for %s, should be %s"%(len(self.text),
							      self.label,
							      HDR_LABELLEN)


class Label1(Label):

    def __init__(self, file_id, file_set_id, file_section_number, 
		 file_seq_number, gen_number, gen_ver_number, creation_date,
		 expiration_date, file_access, block_count, implementation_id):
	Label.__init__(self)
	self.file_id = file_id[0:FILENAMELEN1]
	# pad the string to be 17 bytes long
	self.file_id = add_r_padding(self.file_id, FILENAMELEN1)
	self.file_set_id = file_set_id
	self.file_section_number = file_section_number
	self.file_seq_number = file_seq_number
	self.gen_number = gen_number
	self.gen_ver_number = gen_ver_number
	self.creation_date = get_date(creation_date)
	self.expiration_date = get_date(expiration_date)
	self.file_access = file_access
	self.implementation_id = implementation_id
	self.reserved = 7*SPACE
	self.block_count = block_count

    def __repr__(self):
	# format ourselves to be a string of length 80
	self.text = "%s%s%s%s%s%s%s%s%s%s%s%s%s"%(self.label, self.file_id, 
						  self.file_set_id,
						  self.file_section_number, 
						  self.file_seq_number, 
						  self.gen_number,
						  self.gen_ver_number, 
						  self.creation_date,
						  self.expiration_date, 
						  self.file_access,
						  self.block_count, 
						  self.implementation_id,
						  self.reserved)
	self.text_len()
	return self.text


class Label2(Label):

    def __init__(self, record_format, block_length, record_length, 
		 implementation_id, offset_length):
	Label.__init__(self)
	if record_format not in RECORDFORMAT:
	    raise UNKNOWNRECFORMAT, \
		  "record format not one of %s"%(RECORDFORMAT,)

	self.record_format = record_format
	if long(block_length) <= BLOCK_LEN_LIMIT:
	    self.block_length = block_length
	else:
	    self.block_length = BLOCK_LENGTH_L*ZERO
	if record_length <= RECORD_LEN_LIMIT:
	    self.record_length = record_length
	else:
	    self.record_length = RECORD_LENGTH_L*ZERO
	self.implementation_id = implementation_id
	self.offset_length = offset_length
	self.reserved = 28*SPACE

    def __repr__(self):
	# format ourselves to be a string of length 80
	self.text = "%s%s%s%s%s%s%s"%(self.label, self.record_format, 
				      self.block_length, self.record_length,
				      self.implementation_id, 
				      self.offset_length, self.reserved)
	self.text_len()
	return self.text


class UserLabel1(Label):

    def __init__(self, file_seq_number, block_length, record_length, site, 
                 hostname, drive_mfg, drive_model, drive_serial_num):
        Label.__init__(self)
        self.file_seq_number = file_seq_number  # this must be filled in
        self.file_seq_number = add_l_padding(self.file_seq_number, 10, ZERO)
        self.block_length = block_length        # this must be filled in
        self.block_length = add_l_padding(self.block_length, 10, ZERO)
        self.record_length = record_length
        self.record_length = add_l_padding(self.record_length, 10, ZERO)
        self.site = site
        self.hostname = hostname
        self.drive_mfg = drive_mfg
        self.drive_model = drive_model
        self.drive_serial_num = drive_serial_num 

    def __repr__(self):
        # format ourselves to be a string of length 80
        self.text = "%s%s%s%s%s%s%s%s%s"%(self.label, self.file_seq_number,
                                        self.block_length, self.record_length,
                                        self.site, self.hostname, 
                                        self.drive_mfg, self.drive_model, 
                                        self.drive_serial_num)
        self.text_len()
        return self.text


class UserLabel2(Label):

    def __init__(self, file_id, nameserver_host, absolute_mode, 
		 uid, gid, file_size):
	Label.__init__(self)
	self.file_id = file_id
	self.nameserver_host = nameserver_host
	self.absolute_mode = enstore_functions2.format_mode(absolute_mode)
	self.uid = uid
	self.gid = gid
	self.file_size = file_size
	self.padding = 2*SPACE

    def __repr__(self):
	# format ourselves to be a string of length 80
	self.text = "%s%s%s%s%s%s%s%s"%(self.label, self.file_id, 
					self.nameserver_host,
					self.absolute_mode, self.uid, self.gid,
					self.file_size, self.padding)
	self.text_len()
	return self.text


class UserLabel3(Label):

    def __init__(self, username, experiment, checksum_algorithm, 
		 file_checksum, last_mod):
	Label.__init__(self)
	self.username = username
	self.experiment = experiment
	self.checksum_algorithm = checksum_algorithm
	self.file_checksum = file_checksum
	self.last_mod = last_mod
	self.padding = 23*SPACE

    def update_checksum(self, file_checksum):
	self.file_checksum = file_checksum

    def __repr__(self):
	# format ourselves to be a string of length 80
	self.text = "%s%s%s%s%s%s%s"%(self.label, self.username, self.experiment,
				      self.checksum_algorithm, self.file_checksum,
				      self.last_mod, self.padding)
	self.text_len()
	return self.text


class UserLabel4(Label):

    def __init__(self, copy_num, segment_num, segment_size, checksum_algorithm,
		 segment_checksum, num_of_blocks):
	Label.__init__(self)
	self.copy_num = copy_num
	self.segment_num = segment_num
	self.segment_size = segment_size
	self.checksum_algorithm = checksum_algorithm
	self.segment_checksum = segment_checksum
	self.timestamp = time.strftime("%Y/%m/%d %H:%M:%S",
				       time.gmtime(time.time()))
	self.num_of_blocks = num_of_blocks
	self.padding = 5*SPACE

    def __repr__(self):
	# format ourselves to be a string of length 80
	self.text = "%s%s%s%s%s%s%s%s%s"%(self.label, self.copy_num, 
					  self.segment_num, self.segment_size,
					  self.checksum_algorithm, 
					  self.segment_checksum, self.timestamp,
					  self.num_of_blocks, self.padding)
	self.text_len()
	return self.text


class UserLabelN(Label):

    def __init__(self, filename_chunk):
	Label.__init__(self)
	self.filename_chunk = filename_chunk[0:UHLNFILECHUNK]
	self.filename_chunk = add_r_padding(self.filename_chunk,
					    UHLNFILECHUNK)

    def __repr__(self):
	# format ourselves to be a string of length 80
	self.text = "%s%s"%(self.label,self.filename_chunk)
	self.text_len()
	return self.text


class VOL1(Label):

    # Enstore will use the following fields -
    #        volume_id  : same as volume label
    #        owner_id   : "ENSTORE"
    def __init__(self, volume_id=6*SPACE, volume_access=SPACE,
		 implementation_id=13*SPACE, owner_id=14*SPACE):
	Label.__init__(self)
	self.label = "VOL1"
	self.volume_id = volume_id
	self.volume_access = volume_access
	self.reserved1 = 13*SPACE
	self.implementation_id = implementation_id
	self.owner_id = owner_id
	self.reserved2 = 28*SPACE
	self.label_version = 3

    def __repr__(self):
	# format ourselves to be a string of length 80
	self.text =  "%s%s%s%s%s%s%s%s"%(self.label, self.volume_id,
					 self.volume_access, self.reserved1, 
					 self.implementation_id, 
					 self.owner_id, self.reserved2, 
					 self.label_version)
	self.text_len()
	return self.text

class UVL1(Label):

    def __init__(self, side_number=0):
	Label.__init__(self)
	self.label = "UVL1"
	self.side_number = side_number
	self.padding = 75*SPACE

    def __repr__(self):
	# format ourselves to be a string of length 80
	self.text =  "%s%s%s"%(self.label, self.side_number,
			       self.padding)
	self.text_len()
	return self.text


class HDR1(Label1):

    def __init__(self, file_id, file_set_id, file_section_number, 
		 file_seq_number, gen_number, gen_ver_number, creation_date,
		 expiration_date, file_access, block_count, implementation_id):
	Label1.__init__(self, file_id, file_set_id, file_section_number, 
			file_seq_number, gen_number, gen_ver_number, 
			creation_date, expiration_date, file_access, 
			block_count, implementation_id)
	self.label = "HDR1"


class EOF1(Label1):
    def __init__(self, file_id, file_set_id, file_section_number, 
		 file_seq_number, gen_number, gen_ver_number, creation_date,
		 expiration_date, file_access, block_count,
		 implementation_id):
	Label1.__init__(self, file_id, file_set_id, file_section_number, 
			file_seq_number, gen_number, gen_ver_number, 
			creation_date, expiration_date, file_access,
			block_count, implementation_id)
	self.label = "EOF1"
	self.block_count = block_count


class HDR2(Label2):

    def __init__(self, record_format, block_length, record_length, 
		 implementation_id, offset_length):
	Label2.__init__(self, record_format, block_length, record_length,
			implementation_id, offset_length)
	self.label = "HDR2"


class EOF2(Label2):

    def __init__(self, record_format, block_length, record_length,
		 implementation_id, offset_length):
	Label2.__init__(self, record_format, block_length, record_length,
			implementation_id, offset_length)
	self.label = "EOF2"
   

class UHL1(UserLabel1):

    def __init__(self, file_seq_number, block_size, record_length, site, 
		 hostname, drive_mfg, drive_model, drive_serial_num):
	UserLabel1.__init__(self, file_seq_number, block_size, record_length,
			    site, hostname, drive_mfg, drive_model, 
			    drive_serial_num)
	self.label = "UHL1"


class UTL1(UserLabel1):

    def __init__(self, file_seq_number, block_size, record_length, site, 
		 hostname, drive_mfg, drive_model, drive_serial_num):
	UserLabel1.__init__(self, file_seq_number, block_size, record_length,
			    site, hostname, drive_mfg, drive_model, 
			    drive_serial_num)
	self.label = "UTL1"


class UHL2(UserLabel2):

    def __init__(self, file_id, nameserver_host, absolute_mode, uid, 
		 gid, file_size):
	UserLabel2.__init__(self, file_id, nameserver_host, 
			    absolute_mode, uid, gid, file_size)
	self.label = "UHL2"


class UTL2(UserLabel2):

    def __init__(self, file_id, nameserver_host, absolute_mode, uid, 
		 gid, file_size):
	UserLabel2.__init__(self, file_id, nameserver_host,
			    absolute_mode, uid, gid, file_size)
	self.label = "UTL2"

class UHL3(UserLabel3):

    def __init__(self, username, experiment, checksum_algorithm,
		 file_checksum, last_mod):
	UserLabel3.__init__(self, username, experiment, 
			    checksum_algorithm, file_checksum,
			    last_mod)
	self.label = "UHL3"


class UTL3(UserLabel3):

    def __init__(self, username, experiment, checksum_algorithm,
		 file_checksum, last_mod):
	UserLabel3.__init__(self, username, experiment, 
			    checksum_algorithm, file_checksum, 
			    last_mod)
	self.label = "UTL3"


class UHL4(UserLabel4):

    def __init__(self, copy_num, segment_num, segment_size, 
		 checksum_algorithm, segment_checksum, num_of_blocks):
	UserLabel4.__init__(self, copy_num, segment_num, segment_size,
			    checksum_algorithm, segment_checksum, num_of_blocks)
	self.label = "UHL4"

class UTL4(UserLabel4):

    def __init__(self, copy_num, segment_num, segment_size, 
		 checksum_algorithm, segment_checksum, num_of_blocks):
	UserLabel4.__init__(self, copy_num, segment_num, segment_size,
			    checksum_algorithm, segment_checksum,
			    num_of_blocks)
	self.label = "UTL4"


class UHLN(UserLabelN):

    def __init__(self, label, filename_chunk):
	UserLabelN.__init__(self, filename_chunk)
	self.label = "UHL%s"%(label,)


class UTLN(UserLabelN):

    def __init__(self, label, filename_chunk):
	UserLabelN.__init__(self, filename_chunk)
	self.label = "UTL%s"%(label,)


class EnstoreLargeFileWrapper:

    def __init__(self, ticket):

	# LEGEND:     NU - not used
	#             FN - fermi specific setting
	#             CN - cern specific setting

	# HDR1/EOF1
	self.filename = ticket.get(PNFSFILENAME,
				   '???')         # CN, FN - file identifier
	self.filename_len = len(self.filename)    # not in hdr, but used here
	if self.filename_len > MAXFILENAMELEN:
	    raise INVALIDLENGTH, "filename too long (%s)"%(self.filename_len,)
	self.file_set_id = 6*ZERO                 # NU - identify file set
	self.file_section_number = 4*ZERO         # NU - for files that span 
	                                          #      tapes, enstores' don't
	self.file_seq_number = 4*ZERO             # NU - num of file in set
	self.gen_number = "%s%s"%(3*ZERO, ONE)    # NU
	self.gen_ver_number = 2*ZERO              # NU
	self.creation_date = time.time()          # CN, FN - when data created
	self.expiration_date = MINUS1             # CN - when data obsolete
	self.file_access = SPACE                  # CN, FN - no access 
	                                          #           restrictions
	self.block_count = 6*ZERO                 # CN, FN - set in eof1_label
	self.implementation_1 = ticket.get(ENCPVERSION,
					   "")    # CN, FN - ENCP version
	# replace the words "CVS $Revision: " with blanks
	self.implementation_1 = string.replace(self.implementation_1, CVSREVISION, "")
	self.implementation_1 = add_r_padding(self.implementation_1[:ENCPVERSION_L], 
					      ENCPVERSION_L)

	# HDR2/EOF2
	self.record_format = RECORDFORMAT[VARIABLE]# CN, FN - variable length 
	                                          #            records
	self.block_length = ticket.get(BLOCKLEN,  # CN, FN - set to 0 if 
	                               "")        #                > 99999
	self.block_length = add_l_padding(self.block_length, BLOCK_LENGTH_L, ZERO)
	# the enstore block length is the same size as the record length
	self.record_length = self.block_length    # CN, FN - set to 0 if 
					          #              > 99999
	compression = ticket.get(COMPRESSION, 0)
	if compression == 0:
	    comp = SPACE
        else:
	    comp = COMPRESSION_USED
	self.implementation_2 = "%s%s%s"%(19*SPACE, comp,
					  15*SPACE)# CN, FN - byte 35 : tape
	                                          #        recording technique
                                                  #        P means drive
						  #        compression used
	self.offset_length = 2*ZERO               # NU

	# UHL1/UTL1
	self.site = FERMILAB                      # CN, FN
	self.hostname = ticket.get(MOVERNODE, "") # CN, FN - where mover runs
	self.hostname = add_r_padding(self.hostname[:HOSTNAME_L], HOSTNAME_L)
	self.drive_mfg = ticket.get(DRIVEMFG, "") # CN, FN - drive manufacturer
	self.drive_mfg = add_r_padding(self.drive_mfg[:DRIVE_MFG_L], DRIVE_MFG_L)
	self.drive_model = ticket.get(DRIVEMODEL, # CN, FN - drive model
				      "")
	self.drive_model = add_r_padding(self.drive_model[:DRIVE_MODEL_L], 
					 DRIVE_MODEL_L)
	self.drive_serial_number = ticket.get(DRIVESERIALNUMBER, # CN, FN
					      "")
	self.drive_serial_number = add_r_padding(self.drive_serial_number[:DRIVE_SERIAL_NUMBER_L],
						 DRIVE_SERIAL_NUMBER_L)

	# UHL2/UTL2
	self.file_id = ticket.get(BFID, "")        # CN, FN (bfid)
	self.file_id = add_l_padding(self.file_id[:BFID_L], BFID_L, SPACE)
	self.nameserver_host = 10*SPACE            # CN
	self.mode = ticket.get(MODE, "")           # CN, FN - file access mode
	self.uid = ticket.get(UID, "")             # CN, FN - uid
	self.uid = add_r_padding(self.uid[:UID_L], UID_L)
	self.gid = ticket.get(GID, "")             # CN, FN - gid
	self.gid = add_r_padding(self.gid[:GID_L], GID_L)
	self.file_size = ticket.get(SIZEBYTES, "")# CN, FN - 64 bit file size
	self.file_size = add_l_padding(self.file_size[:FILE_SIZE_L], 
				       FILE_SIZE_L, ZERO)

	# UHL3/UTL3
	self.username =  ticket.get(USERNAME, "") # CN, FN
	self.username = add_r_padding(self.username[:USERNAME_L],
				      USERNAME_L)
	self.experiment = ticket.get(EXPERIMENT, "")# CN, FN
	self.experiment = add_r_padding(self.experiment[:EXPERIMENT_L],
					EXPERIMENT_L)
	self.checksum_algorithm = ADLER32         # CN, FN - AD = Adler32, 
	                                          #          CS = cksum
	self.file_checksum = ticket.get(CHECKSUM, # CN, FN - file checksum 
					"")       #          (32 bits)
	self.file_checksum = add_l_padding(self.file_checksum[:FILE_CHECKSUM_L],
					   FILE_CHECKSUM_L, ZERO)
	self.last_mod = 19*SPACE                  # CN - last modification
	                                          #          date/time

	# UHL4/UTL4
	self.copy_num = 5*ZERO                    # CN
	self.segment_num = 5*ZERO                 # CN
	self.segment_size = 20*ZERO               # CN
	self.segment_checksum_algorithm = ADLER32 # CN
	self.segment_checksum = 10*ZERO           # CN
	self.num_of_blocks = 10*ZERO              # CN

	self.uhln_l = []
	self.utln_l = []

    # assemble the extra uhlns
    def assemble_uhlns(self, uhln_l):
	rtn = ""
	for uhln in uhln_l:
	    rtn = "%s%s"%(rtn, uhln)
	else:
	    return rtn

    # assemble the hdr_labels
    def assemble_hdr_labels(self):
	return "%s%s%s%s%s%s%s"%(self.hdr1, self.hdr2, self.uhl1, self.uhl2,
				 self.uhl3, self.uhl4, 
				 self.assemble_uhlns(self.uhln_l))

    # assemble the eof_labels
    def assemble_eof_labels(self):
	return "%s%s%s%s%s%s%s"%(self.eof1, self.eof2, self.utl1, self.utl2,
				 self.utl3, self.utl4, 
				 self.assemble_uhlns(self.utln_l))

    # get filename objects
    def get_filename_objects(self, aList, aClass):
	if self.filename_len > FILENAMELEN1:
	    # we need to store the rest of the file name
	    i = 0
	    findex = FILENAMELEN1
	    while findex <= self.filename_len:
		aList.append(aClass(string.uppercase[i],
				  self.filename[findex:]))
		i = i + 1
		findex = findex + UHLNFILECHUNK

    # make the hdr_labels
    def hdr_labels(self):
	self.hdr1 = HDR1(self.filename, self.file_set_id, 
			 self.file_section_number, self.file_seq_number,
			 self.gen_number, self.gen_ver_number,
			 self.creation_date, self.expiration_date,
			 self.file_access, self.block_count,
			 self.implementation_1)
	self.hdr2 = HDR2(self.record_format, self.block_length,
			 self.record_length, self.implementation_2,
			 self.offset_length)
	self.uhl1 = UHL1(self.file_seq_number, self.block_length, 
			 self.record_length, self.site, self.hostname, 
			 self.drive_mfg, self.drive_model, 
			 self.drive_serial_number)
	self.uhl2 = UHL2(self.file_id, self.nameserver_host,
			 self.mode, self.uid, self.gid,
			 self.file_size)
	self.uhl3 = UHL3(self.username, self.experiment, self.checksum_algorithm,
			 self.file_checksum, self.last_mod)
	self.uhl4 = UHL4(self.copy_num, self.segment_num, self.segment_size,
			 self.segment_checksum_algorithm, 
			 self.segment_checksum, self.num_of_blocks)
	self.uhln_l = []
	self.get_filename_objects(self.uhln_l, UHLN)

	return self.assemble_hdr_labels()

    # make the eof_labels
    def eof_labels(self):
	self.eof1 = EOF1(self.filename, self.file_set_id, 
			 self.file_section_number, self.file_seq_number,
			 self.gen_number, self.gen_ver_number,
			 self.creation_date, self.expiration_date,
			 self.file_access, self.block_count, 
			 self.implementation_1)
	self.eof2 = EOF2(self.record_format, self.block_length,
			 self.record_length, self.implementation_2,
			 self.offset_length)
	self.utl1 = UTL1(self.file_seq_number, self.block_length, 
			 self.record_length, self.site, self.hostname, 
			 self.drive_mfg, self.drive_model,
			 self.drive_serial_number)
	self.utl2 = UTL2(self.file_id, self.nameserver_host,
			 self.mode, self.uid, self.gid,
			 self.file_size)
	self.utl3 = UTL3(self.username, self.experiment, 
			 self.checksum_algorithm,
			 self.file_checksum, self.last_mod)
	self.utl4 = UTL4(self.copy_num, self.segment_num, self.segment_size,
			 self.segment_checksum_algorithm,			 
			 self.segment_checksum, self.num_of_blocks)
	self.utln_l = []
	self.get_filename_objects(self.utln_l, UTLN)

	return self.assemble_eof_labels()

# here starts the routines accessed via the user interface

# construct the hdr_labels
def hdr_labels(ticket):
    global efile
    efile = EnstoreLargeFileWrapper(ticket)
    return efile.hdr_labels()

# construct the eof_labels
def eof_labels(file_checksum):
    global efile
    if efile:
	efile.file_checksum = "%s"%(file_checksum,)
	efile.file_checksum = add_l_padding(efile.file_checksum[:FILE_CHECKSUM_L],
					    FILE_CHECKSUM_L, ZERO)
	return efile.eof_labels()
    else:
	return ""

# the cern wrapper does not supply headers
def headers(dummy):
    global efile
    print efile.file_size, long(efile.file_size)
    pad = long(efile.file_size) % 512
    if pad:
        pad = int(512 - pad) #Note: python 1.5 doesn't allow string*long
        trailer = '\0'*pad
    return "", trailer
    
    return "", ""

min_header_size = 0
# return the size of the headers
def header_size(hdr):
    return 0

MOVER = 'mover'

def create_wrapper_dict(ticket):
    # the wrapper section already contains some of the information that we need

    # we need these as strings
    wrapper_d = {}
    wrapper_d[UID] = "%s"%(ticket['wrapper'][UID],)
    wrapper_d[GID] = "%s"%(ticket['wrapper'][GID],)
    wrapper_d[MODE] = ticket['wrapper'][MODE]
    wrapper_d[SIZEBYTES] = "%s"%(ticket['wrapper'][SIZEBYTES],)
    wrapper_d[BLOCKLEN] = "%s"%(ticket['vc'].get('blocksize', ""),)
    wrapper_d[BFID] = "%s"%(ticket['fc'].get(BFID, ""),)
    wrapper_d[PNFSFILENAME] = "%s"%(ticket['wrapper'][PNFSFILENAME],)
    wrapper_d[USERNAME] = "%s"%(ticket['wrapper'][USERNAME],)
    wrapper_d[EXPERIMENT] = "%s"%(ticket['pnfs']['storage_group'],)
    wrapper_d[ENCPVERSION] = ticket[ENCPVERSION]

    ticket_mover = ticket[MOVER]
    wrapper_d[COMPRESSION] = "%s"%(ticket_mover[COMPRESSION])
    wrapper_d[MOVERNODE] = ticket_mover[MOVERNODE]
    wrapper_d[DRIVEMFG] = ticket_mover[DRIVEMFG]
    wrapper_d[DRIVEMODEL] = ticket_mover[DRIVEMODEL]
    wrapper_d[DRIVESERIALNUMBER] = ticket_mover[DRIVESERIALNUMBER]
    
    return wrapper_d

def vol_labels(vol_id, own_id=OWNER):
    vol1 = VOL1(volume_id=vol_id, owner_id=own_id)
    uvl1 = UVL1()
    return "%s%s"%(vol1, uvl1)


if __name__ == '__main__':            

    ticket = {'work': 'read_from_hsm', 
	      'unique_id': 'd0test.fnal.gov-984143431.780720-6625820', 
	      'retry_cnt': 0, 
	      'at_the_top': 68, 
	      'encp': {'agetime': 15, 'delayed_dismount': 0, 'adminpri': -1, 'curpri': 8, 
		       'basepri': 1, 'delpri': 1}, 
	      'fc': {'size': 1184489965L, 'sanity_cookie': (65536L, 3097451647L), 
		     'bfid': '98218808600000', 'location_cookie': '0000_000000000_0000042', 
		     'address': ('131.225.164.21', 7501), 
		     'pnfsid': '0009000000000000000ECAA0', 
		     'pnfs_mapname': '/pnfs/sam/m2/volmap/test_harness/PRG204/0000_000000000_0000042', 
		     'drive': 'd0enmvr13a:/dev/rmt/tps0d2n:0062037155', 
		     'external_label': 'PRG204', 'deleted': 'no', 
		     'pnfs_name0': '/pnfs/sam/m2/test_harness/dehong/gen.rate_test_file_n_nm2_11.0007_001', 
		     'pnfsvid': '000A000000000000002833E0', 'complete_crc': 1325401188L}, 
	      'times': {'t0': 984143431.56400001, 'in_queue': 6303.9209929704666, 
			'job_queued': 984143431.91466701}, 
	      'wrapper': {'minor': 2, 'inode': 0, 
			  'fullname': '/sam/test3/sam/dev/d0-test-station/boo/gen.rate_test_file_n_nm2_11.0007_001', 
			  'size_bytes': 1184489965L, 'gname': 'g023', 'mode': 33252, 
			  'gid': 1530, 'sanity_size': 65536, 
			  'machine': ('IRIX64', 'd0test', '6.5', '01101245', 'IP27'), 
			  'uname': 'sam', 
			  'pstat': (33188, 151964320L, 1310722, 1, 7816, 1530, 1184489965L, 982188105, 982188105, 982187564), 
			  'uid': 7816, 
			  'pnfsFilename': '/pnfs/sam/m2/test_harness/dehong/gen.rate_test_file_n_nm2_11.0007_001', 
			  'rminor': 0, 'rmajor': 0, 'major': 0}, 
	      'lm': {'address': ('131.225.164.84', 7515)}, 
	      'callback_addr': ('131.225.224.77', 55568),
	      'mover': {'compression':0, 'host': 'happy', 'vendor_id':'EXABYTE',
			'product_id':'EXB-89008E000112', 'serial_num': '0060112307'},
	      'reject_reason': ('LIMIT_REACHED', None), 
	      'pnfs': {'file_family': 'alex', 'file_family_width': 2,
		       'file_family_wrapper': 'cern', 'library': 'mam',
		       'storage_group': 'G1'},
	      'vc': {'first_access': 982176057.15699995, 'sum_rd_err': 2, 
		     'last_access': 984142192.78699994, 'media_type': '8MM', 
		     'volume_family': 'D0.test_harness.cpio_odc', 
		     'address': ('131.225.164.21', 7502), 'capacity_bytes': 64424509440L, 
		     'blocksize': 131072, 'sum_wr_access': 44, 
		     'eod_cookie': '0000_000000000_0000045', 'remaining_bytes': 922487808L, 
		     'library': 'samm2', 'sum_wr_err': 0, 'external_label': 'PRG204', 
		     'non_del_files': 44, 'system_inhibit': ['none', 'full'], 
		     'current_location': '0000_000000000_0000042', 
		     'user_inhibit': ['none', 'none'], 'wrapper': 'cpio_odc', 
		     'declared': 978726166.42200005, 'sum_rd_access': 341, 
		     'status': ('ok', None)}, 
	      'version': 'v2_14 CVS $Revision$ ',
	      'status': ('ok', None)}

    vol_l = vol_labels("PRL333")
    wrapper_d = create_wrapper_dict(ticket)
    hdr_l = hdr_labels(wrapper_d)
    eof_l = eof_labels(123456789L)
    print "%s%s%s"%(vol_l, hdr_l, eof_l)
