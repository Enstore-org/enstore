import time

SPACE = " "
MINUS1 = -1
ZERO = "0"
RECORD_FORMAT = ['F', 'D', 'S']
FIXED = 0       # index in to RECORD_FORMAT
VARIABLE = 1    # index in to RECORD_FORMAT
SEGMENTED = 2   # index in to RECORD_FORMAT
HEADERLEN = 80
TRAILERLEN = HEADERLEN
FILENAMELEN1 = 17
UHLNFILECHUNK = HEADERLEN - 4
MAXFILENAMELEN = (26*UHLNFILECHUNK)+FILENAMELEN1
FERMILAB = "Fermilab  "  # this string must remain 10 bytes long

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
	date_l = time.localtime(now)
	cent, cent_digit = get_cent_digit(date_l)
	# the format is cent_digit then year (2 chars), then julian day.
	# make the julian day be 3 chars long with preceeding 0's
	return "%s%s%s"%(cent_digit, 
			 string.replace("%2s"%(date_l[0] - cent,), " ", "0"),
			 string.replace("%3s"%(date_l[7],), " ", "0"))
    else:
	# this format means no time was specified
	cent, cent_digit = get_cent_digit(time.localtime(time.time()))
	return "%s%s"%(cent_digit, 5*ZERO)


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
		 file_seq_number, gen_number, gen_ver_number, expiration_date,
		 file_access):
	Label.__init__(self)
	self.file_id = file_id[0:FILENAMELEN1]
	# pad the string to be 17 bytes long
	if len(file_id) < FILE_ID_SIZE:
	    self.file_id = "%s%s"%(self.file_id, 
				   (FILE_ID_SIZE - len(file_id))*SPACE)
	self.file_set_id = file_set_id
	self.file_section_number = file_section_number
	self.file_seq_number = file_seq_number
	self.gen_number = gen_number
	self.gen_ver_number = gen_ver_number
	self.creation_date = get_date(time.time())
	self.expiration_date = get_date(expiration_date)
	self.file_access = file_access
	self.implement_id = 13*SPACE
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
						  self.implement_id,
						  self.reserved)
	self.text_len()
	return self.text


class Label2(Label):

    def __init__(self, record_format, block_length, record_length, 
		 offset_length):
	self.label = "HDR2"
	if record_format not in RECORD_FORMAT:
	    raise UNKNOWNRECFORMAT, \
		  "record format not one of %s"%(RECORD_FORMAT,)

	self.record_format = record_format
	self.block_length = block_length
	self.record_length = record_length
	self.implementation = 35*SPACE
	self.offset_length = offset_length
	self.reserved = 28*SPACE

    def __repr__(self):
	# format ourselves to be a string of length 80
	self.text = "%s%s%s%s%s%s%s"%(self.label, self.record_format, 
				      self.block_length, self.record_length, 
				      self.implementation, self.offset_length,
				      self.reserved)
	self.text_len()
	return self.text


class UserLabel1(Label):

    def __init__(self, file_seq_number, block_size, site, hostname, drive_mfg,
		 drive_model, drive_serial_num):
	self.file_seq_number = file_seq_number  # this must be filled in
	self.block_size = block_size            # this must be filled in
	self.site = site
	self.hostname = hostname
	self.drive_mfg = drive_mfg
	self.drive_model = drive_model
	self.drive_serial_num = drive_serial_num 

    def __repr__(self):
	# format ourselves to be a string of length 80
	self.text = "%s%s%s%s%s%s%s%S"%(self.label, self.file_seq_number,
					self.block_size, self.site, 
					self.hostname, self.drive_mfg,
					self.drive_model, 
					self.drive_serial_num)
	self.text_len()
	return self.text


class UserLabel2(Label):

    def __init__(self, file_id, absolute_mode, uid, gid, file_size,
		 file_checksum):
	self.file_id = file_id
	self.absolute_mode = absolute_mode
	self.uid = uid
	self.gid = gid
	self.file_size = file_size
	self.file_checksum = file_checksum
	self.padding = 2*SPACE

    def __repr__(self):
	# format ourselves to be a string of length 80
	self.text = "%s%s%s%s%s%s%s%s"%(self.label, self.file_id, 
					self.absolute_mode, self.uid, self.gid,
					self.file_size, self.file_checksum,
					self.padding)
	self.text_len()
	return self.text


class UserLabel3:

    def __init__(self, username, experiment, last_mod):
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


class UserLabel4:

    def __init__(self, copy_num, segment_num, segment_size, segment_checksum, 
		 timestamp, num_of_blocks=10*ZERO):
	self.copy_num = copy_num
	self.segment_num = segment_num
	self.segment_size = segment_size
	self.segment_checksum = segment_checksum
	self.timestamp = timestamp
	self.num_of_blocks = num_of_blocks
	self.padding = 2*SPACE

    def __repr__(self):
	# format ourselves to be a string of length 80
	self.text = "%s%s%s%s%s%s%s%s"%(self.label, self.copy_num, 
					self.segment_num, self.segment_size,
					self.segment_checksum, self.timestamp,
					self.num_of_blocks, self.padding)
	self.text_len()
	return self.text


class UserLabelN:

    def __init__(self, filename_chunk):
	self.filename_chunk = filename_chunk

    def __repr__(self):
	# format ourselves to be a string of length 80
	self.text = self.filename_chunk
	self.text_len()
	return self.text


class VOL1:

    def __init__(self, volume_id=6*SPACE, volume_access=SPACE):
	self.label = "VOL1"
	self.volume_id = volume_id
	self.volume_access = volume_access
	self.implementation_id = 13*SPACE
	self.owner_id = 14*SPACE
	self.label_version = 4


class HDR1(Label1):

    def __init__(self, file_id, file_set_id, file_section_number, 
		 file_seq_number, gen_number, gen_ver_number, expiration_date,
		 file_access):
	Label1.__init__(self, file_id, file_set_id, file_section_number, 
			file_seq_number, gen_number, gen_ver_number, 
			expiration_date, file_access)
	self.label = "HDR1"


class EOF1(Label1):
    def __init__(self, file_id, file_set_id, file_section_number, 
		 file_seq_number, gen_number, gen_ver_number, expiration_date,
		 block_count, file_access, block_count):
	Label1.__init__(self, file_id, file_set_id, file_section_number, 
			file_seq_number, gen_number, gen_ver_number, 
			expiration_date, file_access)
	self.label = "EOF1"
	self.block_count = block_count


class HDR2(Label2):

    def __init__(self, record_format, block_length, record_length, 
		 offset_length):
	Label2.__init__(self, record_format, block_length, record_length,
			offset_length)
	self.label = "HDR2"


class EOF2(Label2):

    def __init__(self, record_format, block_length, record_length,
		 offset_length):
	Label2.__init__(self, record_format, block_length, record_length,
			offset_length)
	self.label = "EOF2"
   

class UHL1(UserLabel1):

    def __init__(self, file_seq_number, block_size, site, hostname, drive_mfg,
		 drive_model, drive_serial_num):
	UserLabel1.__init__(self, file_seq_number, block_size, site, hostname,
			    drive_mfg, drive_model, drive_serial_num)
	self.label = "UHL1"


class UTL1(UserLabel1):

    def __init__(self, file_seq_number, block_size, site, hostname, drive_mfg,
		 drive_model, drive_serial_num):
	UserLabel1.__init__(self, file_seq_number, block_size, site, hostname,
			    drive_mfg, drive_model, drive_serial_num)
	self.label = "UTL1"


class UHL2(UserLabel2):

    def __init__(self, absolute_mode, uid, gid, file_size, file_checksum):
	UserLabel2.__init__(self, absolute_mode, uid, gid, file_size, 
			    file_checksum)
	self.label = "UHL2"


class UTL2(UserLabel2):

    def __init__(self, absolute_mode, uid, gid, file_size, file_checksum):
	UserLabel2.__init__(self, absolute_mode, uid, gid, file_size, 
			    file_checksum)
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

    def __init__(self, copy_num, segment_num, segment_size, segment_checksum,
		 timestamp):
	UserLabel4.__init__(self, copy_num, segment_num, segment_size,
			    segment_checksum, timestamp)
	self.label = "UHL4"

class UTL4(UserLabel4):

    def __init__(self, copy_num, segment_num, segment_size, segment_checksum,
		 timestamp, num_of_blocks):
	UserLabel4.__init__(self, copy_num, segment_num, segment_size,
			    segment_checksum, timestamp, num_of_blocks)
	self.label = "UTL4"


class UHLN(UserLabelN):

    def __init__(self, label, filename_chunk):
	UserLabelA.__init__(self, filename_chunk)
	self.label = "UHL%s"%(label,)


class UTLA(UserLabelA):

    def __init__(self, label, filename_chunk):
	UserLabelA.__init__(self, filename_chunk)
	self.label = "UTL%s"%(label,)


class EnstoreFile(File):

    def __init__(self, ticket):

	# LEGEND:     NU - not used
	#             ST - set to default from Standard
	#             FN - fermi specific setting

	# HDR1/EOF1
	self.filename = ticket.get('pnfsFilename', 
				   '???')         # FN - file identifier
	self.filename_len = len(self.filename)    # not in hdr, but used here
	if self.filename_len > MAXFILENAMELEN:
	    raise INVALIDLENGTH, "filename too long (self.filename_len)"
	self.file_set_id = 6*SPACE                # NU - identify file set
	self.file_section_number = 4*ZERO         # NU
	self.file_seq_number = 4*ZERO             # NU - num of file in set
	self.gen_number = 4*ZERO                  # NU
	self.gen_ver_number = 2*ZERO              # NU
	self.expiration_date = MINUS1             # NU, ST - when data obsolete
	self.file_access = SPACE                  # FN - no access restrictions
	self.block_count = 6*ZERO                 # FN - set in trailer

	# HDR2/EOF2
	self.record_format = RECORDFORMAT[FIXED]  # FN - fixed length records
	self.block_length =                       # 
	self.record_length =                      # 
	self.offset_length = 2*ZERO               # NU

	# UHL1/UTL1
	self.file_seq_number =                    #
	self.block_size =                         #
	self.site = FERMILAB                      # FN
	self.hostname = 
	self.drive_mfg = 
	self.drive_model = 
	self.drive_serial_number = 

	# UHL2/UTL2
	self.file_id = 
	self.mode = ticket.get('mode', 0)         # file access mode
	self.uid = ticket.get('uid', 0)           # uid
	self.gid = ticket.get('gid', 0)           # gid
	self.filesize = ticket.get('size_bytes', 0L)   # 64 bit file size
	self.file_checksum = 

	# UHL3/UTL3
	self.username = 
	self.experiment = 
	self.last_mod = 

	# UHL4/UTL4
	self.copy_num = 
	self.segment_num = 
	self.segment_size = 
	self.segment_checksum = 
	self.timestamp = 
	self.num_of_blocks =

    # assemble the extra uhlns
    def assemble_uhlns(self, uhln_l):
	rtn = ""
	for uhln in uhln_l:
	    rtn = "%s%s"%(rtn, uhln)
	else:
	    return rtn

    # assemble the headers
    def assemble_headers(self):
	return "%s%s%s%s%s%s%s"%(self.hdr1, self.hdr2. self.uhl1. self.uhl2,
				 self.uhl3, self.uhl4, 
				 self.assemble_uhlns(self.uhln_l))

    # assemble the trailers
    def assemble_trailers(self):
	return "%s%s%s%s%s%s%s"%(self.eof1, self.eof2. self.utl1. self.utl2,
				 self.utl3, self.utl4, 
				 self.assemble_uhlns(self.utln_l))

    # get filename objects
    def get_filename_objects(self, aList, aClass):
	if self.filename_len > FILENAMELEN1:
	    # we need to store the rest of the file name
	    i = 0
	    findex = FILENAMELEN1
	    while findex <= self.filename_len
		aList.append(aClass(string.uppercase(i),
				  self.filename[findex:]))
		i = i + 1
		findex = findex + UHLNFILECHUNK

    # make the headers
    def headers(self, ticket):
	self.hdr1 = HDR1(self.filename, self.file_set_id, 
			 self.file_section_number, self.file_seq_number,
			 self.gen_number, self.gen_ver_number,
			 self.expiration_date, self.file_access)
	self.hdr2 = HDR2(self.record_format, self.block_length,
			 self.record_length, self.offset_length)
	self.uhl1 = UHL1(self.file_seq_number, self.block_size, self.site,
			 self.hostname, self.drive_mfg, self.drive_model,
			 self.drive_serial_number)
	self.uhl2 = UHL2(self.file_id, self.mode, self.uid, self.gid,
			 self.file_size, self.file_checksum)
	self.uhl3 = UHL3(self.username, self.experiment, self.last_mod)
	self.uhl4 = UHL4(self.copy_num, self.segment_num, self.segment_size,
			 self.segment_checksum, self.timestamp, 
			 self.num_of_blocks)
	self.uhln_l = []
	self.get_filename_objects(self.uhln_l, UHLN)

	return self.assemble_headers()

    # make the trailers
    def trailers(self):
	self.eof1 = EOF1(self.filename, self.file_set_id, 
			 self.file_section_number, self.file_seq_number,
			 self.gen_number, self.gen_ver_number,
			 self.expiration_date, self.file_access, self.block_count)
	self.eof2 = EOF2(self.record_format, self.block_length,
			 self.record_length, self.offset_length)
	self.uhl1 = UHL1(self.file_seq_number, self.block_size, self.site,
			 self.hostname, self.drive_mfg, self.drive_model,
			 self.drive_serial_number)
	self.utl1 = UTL1(self.file_seq_number, self.block_size, self.site,
			 self.hostname, self.drive_mfg, self.drive_model,
			 self.drive_serial_number)
	self.utl2 = UTL2(self.file_id, self.mode, self.uid, self.gid,
			 self.file_size, self.file_checksum)
	self.utl3 = UTL3(self.username, self.experiment, self.last_mod)
	self.utl4 = UTL4(self.copy_num, self.segment_num, self.segment_size,
			 self.segment_checksum, self.timestamp, 
			 self.num_of_blocks)
	self.utln_l = []
	self.get_filename_objects(self.utln_l, UTLN)

	return self.assemble_trailers()

# here starts the routines accessed via the user interface
min_header_size = 14 * 80

# construct the headers and trailers
def headers(ticket):
    efile = EnstoreFile(ticket)
    return efile.headers(), efile.trailers()

# return the size of the headers
def header_size(hdr):
    pass
