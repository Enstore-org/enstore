#!/usr/bin/env python
"""
swap_original_and_copy
"""

# system imports
import sys
import os

# enstore imports
import bfid_util
import duplication_util
import enstore_functions3
import pnfs
import e_errors
import volume_family

dm = duplication_util.DuplicationManager()

def usage():
	print "Pass the original bfid(s) or volume(s)."
	print
	print "Usage:"
	print "%s [[bfid1] [bfid2] ...] | [[vol1] [vol2] ...]" % (sys.argv[0])

def is_copy_file_family(file_family):
	if file_family.find("_copy_") != -1:
		return 1

	return 0

def is_normal_file_family(file_family):
	return not is_copy_file_family(file_family)

def get_normal_file_family(file_family):
	copy_index = file_family.find("_copy_")
	if copy_index == -1:
		#This is already set to normal.
		return file_family

	return  file_family[:copy_index]

def get_copy_file_family(file_family, count):
	copy_index = file_family.find("_copy_")
	if copy_index == -1:
		#This was a normal file family.  Returning the new copy
		# file family.
		return "%s%s%s" % (file_family, "_copy_", count)

	#The file_family contains a copy value.  We are just going to
	# reset it.  It may have been correct or incorrect, either way
	# to work to find out which case it is, is more work than just
	# setting it correct here.
	normal = get_normal_file_family(file_family)
	return "%s%s%s" % (normal, "_copy_", count)


#Internal swap, that does not do any verification or checking itself.
def __swap_bfid(bfid):
	print "swapping %s ..." % (bfid),
	res = dm.swap_original_and_copy(bfid)
	if res:
		print res, "... ERROR"
		return 1
	else:
		print "OK"
		return 0

def swap_bfid(bfid):
	if dm.is_primary(bfid):
		return __swap_bfid(bfid)
	else:
		print "%s is not a primary file" % (bfid,)
		return 1

def swap_volume(vol):
	# check if all files are primary
	q = "select bfid from file, volume where file.volume = volume.id and label = '%s' and deleted = 'n';"%(vol)
	res = dm.db.query(q).getresult()
	for i in range(len(res)):
		bfid = res[i][0]
		if not dm.is_primary(bfid):
			print "%s is not a primary file" % (bfid,)
			return 1

	#Obtain all the volumes that will be swapped with the current
	# tape containing only primary files.
	labels = {}
	highest_copy_count = 0
	for i in range(len(res)):
		bfid = res[i][0]

		#Determine the volume the copy is on for this primary bfid.
		q2 = "select label,storage_group,file_family,wrapper from file,volume,file_copies_map where file.volume = volume.id and file.bfid = file_copies_map.alt_bfid and file_copies_map.bfid = '%s'" % (bfid,)
		res2 = dm.db.query(q2).getresult()
		try:
			label = res2[0][0] #We should never fail in this query.
		except (IndexError, TypeError):
			label = None
		if label and label not in labels.keys():
		    #If we don't already have this in the list.
		    labels[label] = {'file_family' : res2[0][2],
				     'storage_group' : res2[0][1],
				     'wrapper' : res2[0][3],
				     }

		#Determine the highest copy count.  This is to be able to
		# set the best value for # in _copy_#.  We need to subtract
		# the one since the original is included in the response
		# from find_all_copies().
		res3_dict = dm.fcc.find_all_copies(bfid)
		copy_count = len(res3_dict.get("copies", [])) - 1
		if copy_count > highest_copy_count:
			highest_copy_count = copy_count

		print bfid, label, highest_copy_count


	# now, swap it
	for i in range(len(res)):
		bfid = res[i][0]
		rtn = __swap_bfid(bfid)
		if rtn:
			return rtn


	#Swap the file_family so that the new backup copy has the _copy_#.
	vol_info = dm.vcc.inquire_vol(vol, 3, 3)
	if e_errors.is_ok(vol_info):
		storage_group = volume_family.extract_storage_group(vol_info['volume_family'])
		file_family = volume_family.extract_file_family(vol_info['volume_family'])
		wrapper = volume_family.extract_wrapper(vol_info['volume_family'])

		if is_normal_file_family(file_family):
			new_file_family = get_copy_file_family(file_family, highest_copy_count)
			print "Volume %s will have file_family: %s" % \
			      (vol, new_file_family)
			new_volume_family = volume_family.make_volume_family(
				storage_group, new_file_family, wrapper)
			vc_ticket = {}
			vc_ticket['external_label'] = vol
			vc_ticket['volume_family'] = new_volume_family
			reply_ticket = dm.vcc.modify(vc_ticket)
			if not e_errors.is_ok(reply_ticket):
				sys.stderr.write(
					"Failed to set file_family: %s\n" %
					(reply_ticket,))
				return 1
		else:
			print "Volume %s already has %s file_family." % \
			      (vol, file_family)


	#Now swap the volumes that just became primary volumes so they
	# don't contain the _copy_# in the file family.
	for label, volume_family_dict in labels.items():
		storage_group = volume_family_dict['storage_group']
		file_family = volume_family_dict['file_family']
		wrapper = volume_family_dict['wrapper']

		if is_normal_file_family(file_family):
			print "Volume %s already has %s file_family." % \
			      (label, file_family,)
		else:
			new_file_family = get_normal_file_family(file_family)
			print "Volume %s will have file_family: %s" % \
			      (label, new_file_family)
			new_volume_family = volume_family.make_volume_family(
				storage_group, new_file_family, wrapper)
			vc_ticket = {}
			vc_ticket['external_label'] = label
			vc_ticket['volume_family'] = new_volume_family
			reply_ticket = dm.vcc.modify(vc_ticket)
			if not e_errors.is_ok(reply_ticket):
				sys.stderr.write(
					"Failed to set file_family: %s\n" %
					(reply_ticket,))
				return 1
	return 0

def main():
	bfid_list = []
	volume_list = []
	for target in sys.argv[1:]:
		if bfid_util.is_bfid(target):
			bfid_list.append(target)
		elif enstore_functions3.is_volume(target):
			volume_list.append(target)
		else:
			try:
				f = pnfs.File(target)
				if f.bfid:
					bfid_list.append(f.bfid)
				else:
					raise ValueError(target)
			except:
				# abort on error
				#error_log("can not find bfid of", target)
				sys.stderr.write("can not find bfid of %s\n" % (target,))
				return 1


	rtn1 = 0
	rtn2 = 0
	for bfid in bfid_list:
		rtn1 = swap_bfid(bfid)
	for volume in volume_list:
		rtn2 = swap_volume(volume)

	return rtn1 + rtn2

if __name__ == "__main__":   # pragma: no cover
	if len(sys.argv) < 2:
		usage()
		sys.exit()

	if os.geteuid() != 0:
		sys.stderr.write("Must run as user root.\n")
		sys.exit(1)

	sys.exit(main())
