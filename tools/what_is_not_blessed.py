#!/usr/bin/env python

import os
import sys
import pprint
import string


def search_directory(directory):
	print "DIRECTORY", directory
	os.chdir(directory)
	
	dirs_list = []
	
	for file in os.listdir("."):  #directory):
		if file[-1] == "~":
			continue
		elif file[-4:] == ".pyc":
			continue
		elif file[:2] == ".#":
			continue
		elif file[:4] == "core":
			continue
		elif file[-2:] == ".a":
			continue
		elif file[-2:] == ".o":
			continue
		elif file[-3:] == ".so":
			continue
		elif file == "CVS":
			continue
		elif string.find(file, "_wrap") >= 0: #from modules dir.
			continue
		elif os.popen("file %s" % (file,)).readline().find("ELF") >= 0:
			continue
		elif os.path.isdir(file):
			#Remember the directories for later.
			dirs_list.append(os.path.join(directory, file))
			continue
		elif not os.path.isfile(file):
			continue

	        #Get the versions.
		# -h supresses the full text descriptions.
		p = os.popen('cvs log -h %s 2> /dev/null | grep -E "(production:|head:)" | grep -v pre_k' % file)
		data = p.readlines()
		p.close()

		try:
			#Pull out just the version numbers.
			head_version = string.split(data[0])[-1]
		except:
			#On error, proceed so that the file name is listed.
			head_version = ""
		try:
			production_version = string.split(data[1])[-1]
		except:
			#On error, proceed so that the file name is listed.
			production_version = ""
			
		#Skip the listing of up-to-date files.
		if head_version == production_version:
			continue

		#Get the date and user.
		# -N            surpresses the list of symbolic names
		# -r<version>   specifies a specific version
		p = os.popen('cvs log -N -r%s %s 2> /dev/null | grep -E "(date:)" | grep -v pre_k' % (head_version, file))
		head_data = p.readlines()
		p.close()
		#Get the date and user.
		p = os.popen('cvs log -N -r%s %s 2> /dev/null | grep -E "(date:)" | grep -v pre_k' % (production_version, file))
		production_data = p.readlines()
		p.close()
			     
		#Pull out just the version numbers and the user names.
		# The dates are in position 1 and the version is
		# in posistion 4 (with counting beginning with 0).
		# The [:-1] on the user is to remove a trailing
		# semi-colon.
		if head_version:
			try:
				head_date = string.split(head_data[0])[1]
			except:
				head_date = ""
			try:
				head_user = string.split(head_data[0])[4][:-1]
			except:
				head_user = ""
		else:
			head_user = ""
			head_date = ""
		if production_version:
			try:
				production_date = string.split(production_data[0])[1]
			except:
				production_date = ""
			try:
				production_user = string.split(production_data[0])[4][:-1]
			except:
				production_user = ""
		else:
			production_user = ""
			production_date = ""

	        #Print the data
		print file,
		print "head:", head_version, head_date, head_user,
		print "production:", production_version, production_date, \
		      production_user
		
	#for each_dir in dirs_list:
	#	search_directory(each_dir)

	#When done, return to previous working directory.
	os.chdir("..")


if __name__ == '__main__':
	if len(sys.argv) > 1:
	    search_directory(sys.argv[1])
	else:
	    search_directory(os.path.join(os.environ['ENSTORE_DIR'], "src"))
	    search_directory(os.path.join(os.environ['ENSTORE_DIR'], "modules"))
	    search_directory(os.path.join(os.environ['ENSTORE_DIR'], "etc"))
	    search_directory(os.path.join(os.environ['ENSTORE_DIR'], "ups"))
