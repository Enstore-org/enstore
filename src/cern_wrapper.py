import time
import string

# ticket keys used in this module
BLOCKLEN = 'blockLen'
CHECKSUM = 'checksum'
COMPRESSION = 'compression'
DRIVEMFG = 'driveManufacturer'
DRIVEMODEL = 'driveModel'
DRIVESERIALNUMBER = 'driveSerialNumber'
ENCPVERSION = 'encpVersion'
EXPERIMENT = 'experiment'
GID = 'gid'
MODE = 'mode'
MOVERNODE = 'moverNode'
PNFSFILENAME = 'pnfsFilename'
RECORDLEN = 'recordLen'
SIZEBYTES = 'sizeBytes'
UID = 'uid'
USERNAME = 'username'

SPACE = " "
MINUS1 = -1
ZERO = "0"
RECORDFORMAT = ['F', 'D', 'S']
FIXED = 0       # index in to RECORDFORMAT
VARIABLE = 1    # index in to RECORDFORMAT
SEGMENTED = 2   # index in to RECORDFORMAT
HEADERLEN = 80
TRAILERLEN = HEADERLEN
FILENAMELEN1 = 17
UHLNFILECHUNK = HEADERLEN - 4
MAXFILENAMELEN = (26*UHLNFILECHUNK)+FILENAMELEN1
FERMILAB = "Fermilab"  # this string must remain 8 bytes long
BLOCK_LEN_LIMIT = 99999
RECORD_LEN_LIMIT = 99999
ADLER32 = "AD"
USERNAME_L = 14
EXPERIMENT_L = 8

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
	if not len(self.text) == HEADERLEN:
	    # we need to be of length HEADERLEN
	    raise INVALIDLENGTH, \
		  "invalid length (%s) for %s, should be %s"%(len(self.text),
							      self.label,
							      HEADERLEN)


class Label1(Label):

    def __init__(self, file_id, file_set_id, file_section_number, 
		 file_seq_number, gen_number, gen_ver_number, creation_date,
		 expiration_date, file_access, implementation_id):
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
	# this 1 needs to be filled in by the subclass
	self.block_count = 6*ZERO

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
	if block_length <= BLOCK_LEN_LIMIT:
	    self.block_length = block_length
	else:
	    self.block_length = 5*ZERO
	if record_length <= RECORD_LEN_LIMIT:
	    self.record_length = record_length
	else:
	    self.record_length = 5*ZERO
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

    def __init__(self, file_id, absolute_mode, uid, gid, file_size,
		 checksum_algorithm, file_checksum):
	Label.__init__(self)
	self.file_id = file_id
	self.absolute_mode = absolute_mode
	self.uid = uid
	self.gid = gid
	self.file_size = file_size
	self.checksum_algorithm = checksum_algorithm
	self.file_checksum = file_checksum

    def __repr__(self):
	# format ourselves to be a string of length 80
	self.text = "%s%s%s%s%s%s%s%s"%(self.label, self.file_id, 
					self.absolute_mode, self.uid, self.gid,
					self.file_size, 
					self.checksum_algorithm, 
					self.file_checksum)
	self.text_len()
	return self.text


class UserLabel3(Label):

    def __init__(self, username, experiment, last_mod):
	Label.__init__(self)
	self.username = username
	self.experiment = experiment
	self.last_mod = last_mod
	self.padding = 35*SPACE

    def __repr__(self):
	# format ourselves to be a string of length 80
	self.text = "%s%s%s%s%s"%(self.label, self.username, self.experiment,
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
	self.timestamp = time.strftime("%Y/%m/%d %H:%M:%S     ",
				       time.gmtime(time.time()))
	self.num_of_blocks = num_of_blocks

    def __repr__(self):
	# format ourselves to be a string of length 80
	self.text = "%s%s%s%s%s%s%s%s"%(self.label, self.copy_num, 
					self.segment_num, self.segment_size,
					self.checksum_algorithm, 
					self.segment_checksum, self.timestamp,
					self.num_of_blocks)
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


class HDR1(Label1):

    def __init__(self, file_id, file_set_id, file_section_number, 
		 file_seq_number, gen_number, gen_ver_number, creation_date,
		 expiration_date, file_access, implementation_id):
	Label1.__init__(self, file_id, file_set_id, file_section_number, 
			file_seq_number, gen_number, gen_ver_number, 
			creation_date, expiration_date, file_access, 
			implementation_id)
	self.label = "HDR1"


class EOF1(Label1):
    def __init__(self, file_id, file_set_id, file_section_number, 
		 file_seq_number, gen_number, gen_ver_number, creation_date,
		 expiration_date, file_access, block_count,
		 implementation_id):
	Label1.__init__(self, file_id, file_set_id, file_section_number, 
			file_seq_number, gen_number, gen_ver_number, 
			creation_date, expiration_date, file_access,
			implementation_id)
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

    def __init__(self, file_id, absolute_mode, uid, gid, file_size, 
		 checksum_algorithm, file_checksum):
	UserLabel2.__init__(self, file_id, absolute_mode, uid, gid, file_size, 
			    checksum_algorithm, file_checksum)
	self.label = "UHL2"


class UTL2(UserLabel2):

    def __init__(self, file_id, absolute_mode, uid, gid, file_size, 
		 checksum_algorithm, file_checksum):
	UserLabel2.__init__(self, file_id, absolute_mode, uid, gid, file_size, 
			    checksum_algorithm, file_checksum)
	self.label = "UTL2"


class UHL3(UserLabel3):

    def __init__(self, username, experiment, last_mod):
	UserLabel3.__init__(self, username, experiment, last_mod)
	self.label = "UHL3"


class UTL3(UserLabel3):

    def __init__(self, username, experiment, last_mod):
	UserLabel3.__init__(self, username, experiment, last_mod)
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
	self.file_set_id = 6*SPACE                # NU - identify file set
	self.file_section_number = 4*ZERO         # NU - for files that span 
	                                          #      tapes, enstores' don't
	self.file_seq_number = 4*ZERO             # NU - num of file in set
	self.gen_number = 4*ZERO                  # NU
	self.gen_ver_number = 2*ZERO              # NU
	self.creation_date = MINUS1               # CN - when data created
	self.expiration_date = MINUS1             # CN - when data obsolete
	self.file_access = SPACE                  # CN, FN - no access 
	                                          #           restrictions
	self.block_count = 6*ZERO                 # CN, FN - set in trailer
	self.implementation_1 = ticket.get(ENCPVERSION,
					   "")    # CN, FN - ENCP version
	self.implementation_1 = add_r_padding(self.implementation_1, 13)

	# HDR2/EOF2
	self.record_format = RECORDFORMAT[VARIABLE]# CN, FN - variable length 
	                                          #            records
	self.block_length = ticket.get(BLOCKLEN,  # CN, FN - set to 0 if 
	                               "")        #                > 99999
	self.block_length = add_l_padding(self.block_length, 5, ZERO)
	# the enstore block length is the same size as the record length
	self.record_length = self.block_length    # CN, FN - set to 0 if 
					          #              > 99999
	self.implementation_2 = "%s%s%s"%(19*SPACE,
					  ticket.get(COMPRESSION, SPACE),
					  15*SPACE)# CN, FN - byte 35 : tape
	                                          #        recording technique
                                                  #        P means drive
						  #        compression used
	self.offset_length = 2*ZERO               # NU

	# UHL1/UTL1
	self.site = FERMILAB                      # CN, FN
	self.hostname = ticket.get(MOVERNODE, "") # CN, FN - where mover runs
	self.hostname = add_r_padding(self.hostname, 10)
	self.drive_mfg = ticket.get(DRIVEMFG, "") # CN, FN - drive manufacturer
	self.drive_mfg = add_r_padding(self.drive_mfg, 8)
	self.drive_model = ticket.get(DRIVEMODEL, # CN, FN - drive model
				      "")
	self.drive_model = add_r_padding(self.drive_model, 8)
	self.drive_serial_number = ticket.get(DRIVESERIALNUMBER, # CN, FN
					      "")
	self.drive_serial_number = add_r_padding(self.drive_serial_number, 12)

	# UHL2/UTL2
	self.file_id = 20*ZERO                    # CN
	self.mode = ticket.get(MODE, "")           # CN, FN - file access mode
	self.mode = add_l_padding(self.mode, 4, ZERO)
	self.uid = ticket.get(UID, "")             # CN, FN - uid
	self.uid = add_r_padding(self.uid, 10)
	self.gid = ticket.get(GID, "")             # CN, FN - gid
	self.gid = add_r_padding(self.gid, 10)
	self.file_size = ticket.get(SIZEBYTES, "")# CN, FN - 64 bit file size
	self.file_size = add_l_padding(self.file_size, 20, ZERO)
	self.checksum_algorithm = ADLER32         # CN, FN - AD = Adler32, 
	                                          #          CS = cksum
	self.file_checksum = ticket.get(CHECKSUM, # CN, FN - file checksum 
					10*ZERO)  #          (32 bits)
	self.file_checksum = add_l_padding(self.file_checksum, 10, ZERO)

	# UHL3/UTL3
	self.username =  ticket.get(USERNAME, "") # CN, FN
	self.username = add_r_padding(self.username, USERNAME_L)
	self.experiment = ticket.get(EXPERIMENT, "")# CN, FN
	self.experiment = add_r_padding(self.experiment, EXPERIMENT_L)
	self.last_mod = 19*SPACE                  # CN - last modification
	                                          #          date/time

	# UHL4/UTL4
	self.copy_num = 5*ZERO                    # CN
	self.segment_num = 5*ZERO                 # CN
	self.segment_size = 20*ZERO               # CN
	self.segment_checksum_algorithm = ADLER32 # CN
	self.segment_checksum = 10*ZERO           # CN
	self.num_of_blocks = 10*ZERO              # CN

    # assemble the extra uhlns
    def assemble_uhlns(self, uhln_l):
	rtn = ""
	for uhln in uhln_l:
	    rtn = "%s%s"%(rtn, uhln)
	else:
	    return rtn

    # assemble the headers
    def assemble_headers(self):
	return "%s%s%s%s%s%s%s"%(self.hdr1, self.hdr2, self.uhl1, self.uhl2,
				 self.uhl3, self.uhl4, 
				 self.assemble_uhlns(self.uhln_l))

    # assemble the trailers
    def assemble_trailers(self):
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

    # make the headers
    def headers(self):
	self.hdr1 = HDR1(self.filename, self.file_set_id, 
			 self.file_section_number, self.file_seq_number,
			 self.gen_number, self.gen_ver_number,
			 self.creation_date, self.expiration_date,
			 self.file_access, self.implementation_1)
	self.hdr2 = HDR2(self.record_format, self.block_length,
			 self.record_length, self.implementation_2,
			 self.offset_length)
	self.uhl1 = UHL1(self.file_seq_number, self.block_length, 
			 self.record_length, self.site, self.hostname, 
			 self.drive_mfg, self.drive_model, 
			 self.drive_serial_number)
	self.uhl2 = UHL2(self.file_id, self.mode, self.uid, self.gid,
			 self.file_size, self.checksum_algorithm,
			 self.file_checksum)
	self.uhl3 = UHL3(self.username, self.experiment, self.last_mod)
	self.uhl4 = UHL4(self.copy_num, self.segment_num, self.segment_size,
			 self.segment_checksum_algorithm, 
			 self.segment_checksum, self.num_of_blocks)
	self.uhln_l = []
	self.get_filename_objects(self.uhln_l, UHLN)

	return self.assemble_headers()

    # make the trailers
    def trailers(self):
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
	self.utl2 = UTL2(self.file_id, self.mode, self.uid, self.gid,
			 self.file_size, self.checksum_algorithm,
			 self.file_checksum)
	self.utl3 = UTL3(self.username, self.experiment, self.last_mod)
	self.utl4 = UTL4(self.copy_num, self.segment_num, self.segment_size,
			 self.segment_checksum_algorithm,			 
			 self.segment_checksum, self.num_of_blocks)
	self.utln_l = []
	self.get_filename_objects(self.utln_l, UTLN)

	return self.assemble_trailers()

# here starts the routines accessed via the user interface
min_header_size = 14 * 80

# construct the headers and trailers
def headers(ticket):
    efile = EnstoreLargeFileWrapper(ticket)
    return efile.headers(), efile.trailers()

# return the size of the headers
def header_size(hdr):
    pass
