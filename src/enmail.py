# enmail.py -- mail tools for enstore

import os
import string
import StringIO
import mimetools
import MimeWriter
import smtplib

# mail_bin(from_add, to_add, subject, file, msg) -- mail a binary file
#
#	The mail is sent in MIME format.
#	msg is the text body and file will be an attachment in base64
#	to_add is either an e-mail address or a list of them
#
#	Initially, this is for sending M2 dump through e-mail.
#	Yet it is general enoguh to have its own existence

def mail_bin(from_add, to_add, subject, file, msg):

	outf = StringIO.StringIO()		# fake a file in memory
	mwf = MimeWriter.MimeWriter(outf)

	# making up the headers
	mwf.addheader('From', from_add)
	# check if sendto is a list
	if type(to_add) == type([]):
		to_addresses = string.joinfields(to_add, ',')
	else:
		to_addresses = to_add
	mwf.addheader('To', to_addresses)
	mwf.addheader('Subject', subject)
	mwf.addheader('MIME-Version', '1.0')
	mwf.flushheaders()

	# it's going to be multi-part
	mwf.startmultipartbody('mixed')

	# first part is the text message
	msgf = mwf.nextpart()
	msgb = msgf.startbody('TEXT/PLAIN; charset=US-ASCII')
	msgf.flushheaders()
	msgb.write(msg)

	# here comes the binary part
	attf = mwf.nextpart()
	attf.addheader('Content-Transfer-Encoding', 'base64')
	fname = os.path.basename(file)
	attf.addheader('Content-Disposition', 'attachment; filename="'+fname+'"')
	attb = attf.startbody('application/octet-stream; name="'+fname+'"')
	attf.flushheaders()
	
	# encode it using base64
	mimetools.encode(open(file, 'r'), attb, 'base64')
	mwf.lastpart()

	mesg = outf.getvalue()

	server = smtplib.SMTP('localhost')
	# server.set_debuglevel(1)	# no debug please
	server.sendmail(from_add, to_addresses, mesg)
	server.quit()

# mail(from_add, to_add, subject, msg) -- s aimple mail through SMTP

def mail(from_add, to_add, subject, msg):

	if type(to_add) == type([]):
		to_addresses = string.jointfields(to_add, ',')
	else:
		to_addresses = to_add

	mesg = 'Subject: '+subject+'\r\n\r\n'+msg

	server = smtplib.SMTP('localhost')
	# server.set_debuglevel(1)	# no debug please
	server.sendmail(from_add, to_addresses, mesg)
	server.quit()
