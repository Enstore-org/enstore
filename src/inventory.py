#!/usr/bin/env python

#system imports
import sys
import time
import string
import os
import tempfile
import pprint
import cPickle

#user imports
import db
import checkBackedUpDatabases
import configuration_client
import enstore_constants
import alarm_client
import e_errors

mount_limit = {}
acc = None

#Grab the start time.
t0 = time.time()

def get_vq_format_file(output_dir):
    return output_dir + enstore_constants.VQFORMATED

# return filenames
def get_vol_filenames(output_dir):
    if string.find(output_dir, "/dev/stdout") != -1: 
        last_access_file = "/dev/stdout"
        volume_size_file = "/dev/stdout"
        volumes_defined_file = "/dev/stdout"
        volumes_too_many_mounts_file = "/dev/stdout"
        volume_quotas_file = "/dev/stdout"
	volume_quotas_format_file = "/dev/stdout"
        total_bytes_file = "/dev/stdout"
    else:
        last_access_file = output_dir + "LAST_ACCESS"
        volume_size_file = output_dir + "VOLUME_SIZE"
        volumes_defined_file = output_dir + "VOLUMES_DEFINED"
        volumes_too_many_mounts_file = output_dir + "VOLUMES_TOO_MANY_MOUNTS"
        volume_quotas_file = output_dir + "VOLUME_QUOTAS"
	volume_quotas_format_file = get_vq_format_file(output_dir)
        total_bytes_file = output_dir + "TOTAL_BYTES_ON_TAPE"
    return last_access_file, volume_size_file, volumes_defined_file, \
		      volume_quotas_file, volume_quotas_format_file, \
		      total_bytes_file, volumes_too_many_mounts_file

#This is the "magic" class to use when filtering out elements that have the
# same external label in a list.
class match_list:
    def __call__(self, list_element):
        return list_element['external_label'] == self.compare_value
    def __init__(self, thresh):
        self.compare_value = thresh

#Print the command line syntax to the terminal.
def inventory_usage(message = None):
    if message:
        print "\n" + message + "\n"
    print "Usage: " + sys.argv[0] + " [-v volume_file] [-f metadata_file]",
    print "[[-o output_directory] | [-stdout]] [--help]"
    print "   -v=      set the volume file to be inventoried"
    print "   -f=      set the metadata file to be inventoried"
    print "   -o=      set the output directory"
    print "  --volume= set the volume to check (enables --stdout)"
    print "  --stdout  set the output directory to standard out"
    print "  --help    print this message"
    print "See configuration dictionary entries \"backup\" and \"inventory\""\
          " for defaults."

#Take in a long int or int and format it into human readable form.
def format_storage_size(size_in_bytes,mode="GB"):

    if mode == "GB":
        z = size_in_bytes/1024./1024./1024. # GB
        return z,"GB"
  
    #suffix list
    suffix = ("B", "KB", "MB", "GB", "TB", "PB")
    
    #format the remaining bytes. collumn
    volume_size = float(size_in_bytes)
    count = 0
    while long(abs(volume_size) / 1024) > 0:
        volume_size = volume_size / 1024
        count = count + 1


    return volume_size, suffix[count]

#Takes an arbitrary number of arguments which contain directories and creates
# them if they do not exist.  If they already exist, then simply delete
# everything in the directory.
def create_clean_dirs(*dirs):
    for dir in dirs:
        #An empty output directory would be nice.
        if string.find(dir, "/dev/stdout") == -1:
            try:
                os.stat(dir)
            except OSError:
                os.mkdir(dir, 0755)
                
            checkBackedUpDatabases.remove_files(os.listdir(dir),dir)

#Takes an arbitrary number of arguments which contain directories and deletes
# them and their contents.
def cleanup_dirs(*dirs):
    for dir in dirs:
        if string.find(dir, "/dev/stdout") == -1:
            try:
                checkBackedUpDatabases.remove_files(os.listdir(dir), dir)
                os.rmdir(dir)
            except OSError:
                continue

#Read in the information from the authorized tapes file.
def get_authorized_tapes():
    #Determine if the file is really there.
    csc = configuration_client.ConfigurationClient()
    inven = csc.get('inventory',timeout=15,retry=3)
    try:
        auth_file = inven.get('authorized_file', None)
        if auth_file != None:
            authorized_file = open(auth_file, "r")
        else:
            return {}
    except (OSError, IOError, AttributeError), detail:
        print "Authorized file:", detail
        return {}

    #Line by line determine if there is something there that we care about.
    #We are primarily looking for lines containing three element tuples.
    #  They are (storage group, requested # of tapes, authorized# of tapes).
    values = {}
    for line in authorized_file.readlines():
        line = line.strip()
        split = line.split()
        if len(line) == 0 or line[0] == '#': #ignore comments
            pass
        if len(split) == 4: #Normal case of (sg, requested, authorized).
            values[(split[0], split[1])] = (split[2], split[3])
        elif line[:7] == "blanks=": #There should only be one of these.
            values['blanks'] = line[7:]

    return values

#############################################################################
#############################################################################
#The next group of functions read databases into memory.  The first (read_db)
# attempts to read in the entire database in one shot.  This works well for
# some smaller database files.  However this doesn't work for large database
# files, which otherwise cause the OS to thrash greatly.  The second function
# allows for the user to read in a large database by specifying the number of
# records to read in durring the function call.  This means that the caller
# of the function needs to be responsible enough to pass in the opened database
# back to the function (from the second ([1]) tuple argement to the second
# (also [1]) function parameter, along with a sensible value for the number
# of records to read in.

#dbname is a tuple containing in [0] the path and in [1] the file name
def read_db(dbname):
    count=0
    list = []
    try:
        d = db.DbTable(dbname[1], dbname[0], dbname[0], [])
        d.cursor('open')
        t1 = time.time()
        k,v = d.cursor('first')

        while k:
            count=count+1
            v=d[k]
            list.append(v)
            k,v=d.cursor('next')
            if count % 1000 == 0:
                delta = time.time()-t1
                print "%d lines read in at %.1f keys/S in %s." % \
                      (count, count/delta, delta)
        
    except:
        exc, msg, tb = sys.exc_info()
        print "DATABASE",dbname[1],"IS CORRUPT. Current count is",count
        print exc, msg
        return 1

    return list

#dbname is a tuple containing in [0] the path and in [1] the file name
def read_long_db(dbname, d, num):
    count=0
    list = []
    try:

        if d == None:
            d = db.DbTable(dbname[1], dbname[0], dbname[0], [])
            d.cursor('open')
            k,v = d.cursor('first')
        else:
            k,v=d.cursor('next')

        while k:
            count=count+1
            list.append(v)
           
            #When num number of elements has been read in, stop.
            if count % num == 0:
                return list, d

            #Advance to the next only after we are sure that the num bound
            # hasn't been reached.
            k,v=d.cursor('next')
        
    except:
        exc, msg, tb = sys.exc_info()
        print "DATABASE",dbname[1],"IS CORRUPT. Current count is",count
        print exc, msg
        return 1

    return list, d


##############################################################################
##############################################################################

#la = Last Access
#Sort function to be used for sorting the volume list based on the last access
# data field.
def la_sort(one, two):
    if one['last_access'] < two['last_access']:
        return -1
    elif one['last_access'] > two['last_access']:
        return 1
    else:
        if one['external_label'] < two['external_label']:
            return -1
        elif one['external_label'] > two['external_label']:
            return 1
        else:
            return 0

#lc = Location Cookie.
#Sort function to be used for sorting the volume list based on the location
# cookie data field.  This sort function sorts a different type of list
# as compared to the other xx_sort functions.
def lc_sort(one, two):
    split1 = string.split(one)
    split2 = string.split(two)

    if split1[3] < split2[3]:
        return -1
    elif split1[3] > split2[3]:
        return 1
    else:
        return 0

#el = External Label
#Sort function to be used for sorting the volume list based on the external
# label.
def el_sort(one, two):
    if one['external_label'] < two['external_label']:
        return -1
    elif one['external_label'] > two['external_label']:
        return 1
    else:
        return 0

##############################################################################
##############################################################################

#Stream the data to be displayed in the header to the designated file
# descriptor.
def print_header(volume, fd):
    out_string = "Volume:\t\t  " + volume['external_label'] + "\n"
    os.write(fd, out_string)

    if volume['last_access'] == -1:
        os.write(fd, "Last accessed on: Never\n")
    else:
        out_string = "Last accessed on: " + \
              time.asctime(time.localtime(volume['last_access'])) + "\n"
        os.write(fd, out_string)

    out_string = "Bytes free:\t  %6.2f%s\n" % \
                 format_storage_size(volume['remaining_bytes'])
    os.write(fd, out_string)

    out_string = "Bytes written:\t  %6.2f%s\n" % \
                 format_storage_size(volume['capacity_bytes'] -
                                     volume['remaining_bytes'])
    os.write(fd, out_string)

    out_string = "Inhibits:\t  %s+%s\n\n" % \
          (volume['system_inhibit'][0], volume['user_inhibit'][0])
    os.write(fd, out_string)

    out_string = "%10s %15s %15s %22s %7s %s\n" % \
              ('label', 'bfid', 'size', 'location_cookie', 'delflag',
               'original_name')
    os.write(fd, out_string)


#This function print out the footer information.  This data is the
# data from the volume file formated to be easier to read than just printing
# out the dictionary.
def print_footer(volume, fd):
    fields_list = volume.keys()
    fields_list.sort()

    os.write(fd, "\n") #need a blank line for readability in the output

    for key in fields_list:
        #If the data contained in volume[key] surround it with sigle quotes
        # to make it look how a string is normally printed.
        if type(volume[key]) == type(""):
            to_print = "'" + volume[key] + "'"
        else:
            to_print = volume[key]
        
        #Print each field on a seperate line.  Place braces (to suround the
        # data) printed on the first and last lines. 
        if key == fields_list[0]:
            out_string = " {'%s': %s\n" % (key, to_print)
            os.write(fd, out_string)
        elif key == fields_list[-1]:
            out_string = "  '%s': %s}\n\n" % (key, to_print)
            os.write(fd, out_string)
        else:
            out_string = "  '%s': %s\n" % (key, to_print)
            os.write(fd, out_string)


def print_data(volume, fd_temp, fd_data):
    sum_size = 0 #initalize this to avoid errors with empty files
    #From the beginning of the temporary file, read it into memory...
    os.lseek(fd_temp, 0, 0)
    in_string = os.read(fd_temp, 512)
    entire_file_string = ""
    while len(in_string) > 0:
        entire_file_string = entire_file_string + in_string
        in_string = os.read(fd_temp, 512)
        
    #...make sure that there is data in the file (string)...
    if len(entire_file_string) == 0:
        return []
    
    #...then obtain a list of strings, where each line in the file
    # is an element in the list.
    sorted_list = string.split(entire_file_string, "\n")

    #If the last line in the file/list is empty, delete it.
    if sorted_list[-1] == "":
        del sorted_list[-1]
        
    #Sort the list of file lines based on the location_cookie (lc).
    sorted_list.sort(lc_sort)
    
    #Write the data to the appropriate file.
    for line in sorted_list:
        os.write(fd_data, line + "\n")

    return sorted_list

#Print the sums of the file sizes to the file VOLUME_SIZE.  Also, print
# out the expected volume sizes.
def print_volume_size_stats(volume_sums, volume_list, output_file):
    usage_file = open(output_file, "w")
    usage_file.write("%10s %9s %9s %11s %9s %9s %9s\n" % ("Label",
                                                               "Actual",
                                                               "Deleted",
                                                               "Non-deleted",
                                                               "Capacity",
                                                               "Remaining",
                                                               "Expected"))
    volume_list.sort(el_sort)
    for volume in volume_list:
        key = volume['external_label']
        format_tuple = (key,) + \
                       format_storage_size(volume_sums[key][0]) + \
                       format_storage_size(volume_sums[key][1]) + \
                       format_storage_size(volume_sums[key][2]) + \
                       format_storage_size(volume['capacity_bytes']) + \
                       format_storage_size(volume['remaining_bytes']) + \
                       format_storage_size(volume['capacity_bytes'] -
                                           volume['remaining_bytes'])
        format_string = \
           "%10s %7.2f%-2s %7.2f%-2s %9.2f%-2s %7.2f%-2s %7.2f%-2s %7.2f%-2s\n"
        
        usage_file.write(format_string % format_tuple)

    usage_file.close()

#Print the last access info to the output file LAST_ACCESS.
def print_last_access_status(volume_list, output_file):
    volume_list.sort(la_sort)
    la_file = open(output_file, "w")
    for volume in volume_list:
        la_file.write("%f, %s %s\n"
                      % (volume['last_access'],
                         time.asctime(time.localtime(volume['last_access'])),
                         volume['external_label']))
    la_file.close()

def print_volumes_defind_status(volume_list, output_file):
    volume_list.sort(el_sort)
    vd_file = open(output_file, "w")


    vd_file.write("Date this listing was generated: %s\n" % \
                  time.asctime(time.localtime(time.time())))
    
    vd_file.write("%-10s  %-8s %-17s %17s  %012s %-12s\n" %
          ("label", "avail.", "system_inhibit", "user_inhibit",
           "library", "volume_family"))
    
    for volume in volume_list:
        formated_size = format_storage_size(volume['remaining_bytes'])

        vd_file.write("%-10s %6.2f%s (%-08s %08s) (%-08s %08s) %-012s %012s\n"
                      % (volume['external_label'],
                         formated_size[0], formated_size[1],
                         volume['system_inhibit'][0],
                         volume['system_inhibit'][1],
                         volume['user_inhibit'][0],
                         volume['user_inhibit'][1],
                         volume['library'],
                         volume['volume_family']))
    vd_file.close()
    
def print_volume_quotas_status(volume_quotas, authorized_tapes, output_file):
    csc = configuration_client.ConfigurationClient()
    quotas = csc.get('quotas',timeout=15,retry=3)
    order = quotas.get('order', {})

    vq_file = open(output_file, "w")

    vq_file.write("Date this listing was generated: %s\n" % \
                  time.asctime(time.localtime(time.time())))
    
    vq_file.write("   %-15s %-15s %-11s %-12s %-6s %-9s %-10s %-12s %-7s %12s %-12s %-13s %s\n" %
          ("Library", "Storage Group", "Req. Alloc.",
           "Auth. Alloc.", "Quota", "Allocated",
           "Blank Vols", "Written Vols", "Deleted Vols", "Space Used ",
           "Active Files", "Deleted Files", "Unknown Files"))

    if quotas.has_key('libraries'):
        libraries = quotas['libraries'].keys()
    else:
        libraries = []
    quotas = volume_quotas.keys()
    
    top = []
    bottom = []
    for keys in quotas:
        b_order = order.get('bottom', [])
        t_order = order.get('top', [])
        
        if keys in b_order:
            bottom.append(keys)
        elif keys in t_order:
            top.append(keys)
        else:
            for t_order in order.get('top', []):
                if t_order[0] == None and t_order[1] == volume_quotas[keys][1]:
                    top.append(keys)
                elif t_order[1] == None and \
                     t_order[0] == volume_quotas[keys][0]:
                    top.append(keys)
        
            for b_order in order.get('bottom', []):
                if b_order[0] == None and b_order[1] == volume_quotas[keys][1]:
                    bottom.append(keys)
                elif b_order[1] == None and \
                     b_order[0] == volume_quotas[keys][0]:
                    bottom.append(keys)
    middle = []
    for keys in quotas:
        if keys not in top + bottom:
            middle.append(keys)

    count = 0
    for quotas in (top, middle, bottom):
        quotas.sort()
        for keys in quotas:
            count = count + 1
            if keys[0] in libraries and \
                volume_quotas[keys][1] == "none":
                formated_storage_group = "none: emergency"
            else:
                formated_storage_group = volume_quotas[keys][1]
            formated_tuple = (count, volume_quotas[keys][0],) + \
                             (formated_storage_group,) + \
                             authorized_tapes.get(volume_quotas[keys][:2],
                                                  ("N/A", "N/A")) + \
                             volume_quotas[keys][2:7] + \
                             format_storage_size(volume_quotas[keys][7]) + \
                             volume_quotas[keys][8:]
            vq_file.write("%2d %-15s %-15s %-11s %-12s %-6s %-9d %-10d %-12d %-12d %9.2f%-3s %-12d %-13d %d\n"
                          % formated_tuple)
        vq_file.write("\n") #insert newline between sections
    vq_file.close()


def print_volume_quota_sums(volume_quotas, authorized_tapes, output_file,
			    output_format_file):
    vq_file = open(output_file, "a")
    vq_file.write(("-" * 140) + "\n\n")

    vq_format_file = open(output_format_file, "w")

    #Sum up each column for each library and print the results
    library_dict = {}
    library_format_dict = {}
    requested = authorized = 0
    quotas = volume_quotas.keys()
    for key in quotas:
        #Get the current (library, storage_group) out of the dict.
        (l, sg, quota, allocated, blank_v, written_v, deleted_v, used,
            active_f, deleted_f, unknown_f) = volume_quotas[key]

        #For each library total up the numbers.
        try: # total up the number of requested tapes.
            requested = int(authorized_tapes.get((l, sg), (0,) * 2)[0]) + \
                        int(library_dict.get(l, (0,) * 13)[2])
        except ValueError:
            requested = int(library_dict.get(l, (0,) * 13)[2])
        try: # total up the number of authorized tapes.
            authorized = int(authorized_tapes.get((l, sg), (0,) * 2)[1]) + \
                         int(library_dict.get(l, (0,) * 13)[3])
        except ValueError:
            authorized = int(library_dict.get(l, (0,) * 13)[3])
        try:
            quota = int(quota) + int(library_dict.get(l, (0,) * 13)[4])
        except:
            quota = int(library_dict.get(l, (0,) * 13)[4])
        allocated = allocated + library_dict.get(l, (0,) * 13)[5]
        blank_v = blank_v + library_dict.get(l, (0,) * 13)[6]
        written_v =  written_v + library_dict.get(l, (0,) * 13)[7]
        deleted_v =  deleted_v + library_dict.get(l, (0,) * 13)[8]
        used = used + library_dict.get(l, (0,) * 13)[9]
        active_f = active_f + library_dict.get(l, (0,) * 13)[10]
        deleted_f =  deleted_f + library_dict.get(l, (0,) * 13)[11]
        unknown_f = unknown_f + library_dict.get(l, (0,) * 13)[12]

        library_dict[l] = (l, "", requested, authorized, quota, allocated,
                           blank_v, written_v, deleted_v, used, active_f,
                           deleted_f, unknown_f)
	library_format_dict[l] = used

    #Since this info is appened to the same file as the volume quotas, make
    # it have the same format.
    keys = library_dict.keys()
    keys.sort()
    count = 0
    for key in keys:
        count = count + 1
        formated_tuple = (count,) + library_dict[key][0:9] + \
                         format_storage_size(library_dict[key][9]) + \
                         library_dict[key][10:]
        vq_file.write("%2d %-15s %-15s %-11s %-12s %-6s %-9d %-10d %-12d %-12d %9.2f%-3s %-12d %-13d %d\n"
                      % formated_tuple)
	vq_format_file.write("%s %s\n"%(key, library_format_dict[key]))
    vq_file.write("\n") #insert newline between sections

    blanks = authorized_tapes.get('blanks', None)
    if blanks != None:
        vq_file.write("blanks=" + blanks + "\n")
    
    vq_file.close()
    vq_format_file.close()

def print_total_bytes_on_tape(volume_sums, output_file):
    sum = 0
    for line in volume_sums.keys():
        sum = volume_sums[line][0] + sum

    tbot_file = open(output_file, "w")

    tbot_file.write("%.2f %s\n" % format_storage_size(sum))

    tbot_file.close()

##############################################################################
##############################################################################

def create_fd_list(volume_list, output_dir):
    #Create the file descriptor dictionary.
    fd_list = {}
    count = 0
    #Loop through each entry in the list of volumes.  Pull out all files
    # from the file data list on each particular volume.  Output the data
    # in the appropriate format.
    for volume in volume_list:
        if output_dir == None:
            fd = 1
            print "setting file descriptor to 1."
        else:
            file_string = output_dir + volume['external_label']

            count = count + 1
            print count,
            fd = os.open(file_string,
                         os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0666)
            print fd
            if fd == -1:
                print "Error opening file " + file_string + "."
                sys.exit(1)
        
        #Place the fd in the file descriptor dictionary.
        fd_list[volume['external_label']] = fd

    return fd_list

def process_out_string(short_list, fd):
    for vol in short_list:
        label = vol.get('external_label', "unknown")
        bfid = vol.get('bfid', "unknown")
        size = vol.get('size', "unknown")
        lc = vol.get('location_cookie', "unknown")
        deleted = vol.get('deleted', "unknown")
        pnfs = vol.get('pnfs_name0', "unknown")
        
        out_string = "%10s %15s %15s %22s %7s %s\n" % \
                         (label, bfid, size, lc, deleted, pnfs)
        
        #It doesn't matter if the value in out_string was set with or
        # without an exception being thrown.  Just stream it out.
        os.write(fd, out_string)


def parse_time(seconds):
    hour = int(seconds) / 60 / 60
    min = int(seconds) / 60 % 60
    sec = int(seconds) % 60

    out_string = ""

    if hour == 1:
        out_string = "%d hour " % (hour)
    elif hour > 0:
        out_string = "%d hours " % (hour)
        
    if min == 1:
        out_string = "%s%d minute " % (out_string, min)
    elif min > 0:
        out_string = "%s%d minutes " % (out_string, min)
        
    if sec == 1:
        out_string = "%s%d second" % (out_string, sec)
    elif sec >= 0:
        out_string = "%s%d seconds" % (out_string, sec)
        
    return out_string


def verify_volume_sizes(this_volume_data, volume, volume_sizes):
    sum_size = 0
    deleted_size = 0
    non_deleted_size = 0
    #Keep some statistical analysis going:
    try:
        for line in this_volume_data:
            split_string = string.split(line)

            #Count the sum of all the file sizes on a given volume.
            if split_string[2][-1] == "L":
                split_string[2] = split_string[2][0:-1] #remove appending 'L'
            sum_size = sum_size + long(split_string[2])

            #Count the sum of all the file sizes of deleted and non-deleted
            # file seperately.
            if split_string[4] == "yes":
                deleted_size = deleted_size + long(split_string[2])
            elif split_string[4] == "no":
                non_deleted_size = non_deleted_size + long(split_string[2])
    except IndexError:
        sum_size = 0 #This exception occurs on empty files. So, these are zero.
        deleted_size = 0
        non_deleted_string = 0

    volume_sizes[volume['external_label']] = (sum_size, deleted_size,
                                              non_deleted_size)

#Process the volume data and send the output to file.
def verify_volume_quotas(volume_data, volume, volumes_allocated):
    csc = configuration_client.ConfigurationClient()
    quotas = csc.get('quotas',timeout=15,retry=3)

    storage_group = string.split(volume['volume_family'], ".")[0]
    library = volume['library']

    try:
        quota = quotas['libraries'][library][storage_group]
    except KeyError:
        quota = "N/A"
    
    #Since the data of which files are on what volume is already known,
    # that same data can be used here.
    if len(volume_data) == 0:
        blank_vols = 1
        written_vols = 0
    else:
        blank_vols = 0
        written_vols = 1

    #Determine if the volume is deleted.
    if volume['system_inhibit'][0] == "DELETED":
        deleted = 1
    else:
        deleted = 0

    #Determine space used for this volume.  Later sum these numbers up for
    # each library/storage group.
    space_used = volume['capacity_bytes'] - volume['remaining_bytes']

    #Count the number of files in each storage group, that are deleted, active,
    # and unknown.
    num_active_files = 0
    num_deleted_files = 0
    num_unknown_files = 0
    for file in volume_data:
        row = string.split(file)
        if row[4] == "no":
            num_active_files = num_active_files + 1
        elif row[4] == "yes":
            num_deleted_files = num_deleted_files + 1
        else:
            num_unknown_files = num_unknown_files + 1

    #Try to update results for each storage group.  If that fails, it means
    # that it is the first volume of a storage group that has been found.
    # Therefore act accordingly with initalization.
    try:
        v_info = volumes_allocated[(library, storage_group)]

        volumes_allocated[(library, storage_group)] =\
                                   (library,
                                    storage_group,
                                    v_info[2],     #quota
                                    v_info[3] + 1, #volume allocated
                                    v_info[4] + blank_vols,
                                    v_info[5] + written_vols,
                                    v_info[6] + deleted,
                                    v_info[7] + space_used,
                                    v_info[8] + num_active_files,
                                    v_info[9] + num_deleted_files,
                                    v_info[10] + num_unknown_files)

    except KeyError:
        volumes_allocated[(library, storage_group)] = (library,
                                                       storage_group,
                                                       quota, #quota
                                                       1, #volumes allocated
                                                       blank_vols,
                                                       written_vols,
                                                       deleted,
                                                       space_used,
                                                       num_active_files,
                                                       num_deleted_files,
                                                       num_unknown_files)
        
    

##############################################################################
##############################################################################

def sort_inventory(data_file, volume_list, tmp_dir):
    t1 = time.time()
    STEP = 1000        #Number of records to read in at a time.
    data_list = [1]    #List where STEP number of records is placed.
    db = None          #The database needed by read_long_db()
    count_metadata = 0 #keep track of the number of files processed.
    #A cool thing this class is.  It is a class, but it acts like a function
    # later on.  This is how the filter function allows me to pass in
    # different filter values.
    compare = match_list("")

    #While there is still data to process.
    while count_metadata % STEP == 0 and len(data_list):

        #Get the data list in groups of 1000 records.  For the first call,
        # pass None into second parameter, this will tell the function to
        # create the database instance.
        long_read_return = read_long_db(os.path.split(data_file), db, STEP)

        #For readability rename the two parts of the return value.
        data_list = long_read_return[0]
        db = long_read_return[1]

        for volume in volume_list:
            #Determine the full file name path for the output.
            file_string = tmp_dir + volume['external_label']

            #It may seam that always opening a file is a waste.  But, this
            # way there will always be a file for a volume (even empty
            # volumes).
            fd = os.open(file_string,
                         os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0666)
            if fd == -1:
                print "Unable to open tmp file", volume['external_label'],
                print "for writing."
                sys.exit(1)
                
            #This is where the magic of that callable class happends.  I set
            # the compare_value variable and it is automatically referenced
            # when the class is executed like a function in the filter() call.
            compare.compare_value = volume['external_label']
            short_list = filter(compare, data_list)

            #With the short list of files located on the current volume
            # print them out to the corresponding temporary file.
            process_out_string(short_list, fd)

            #Close the open file to avoid opening to many at once.
            os.close(fd)

        #Since, the while loop takes a while to process all of the data,
        # generate some running performace statistics.
        delta = time.time()-t0
        omega = time.time()-t1
        count_metadata = count_metadata + len(data_list)
        print "%d lines read in at %.1f keys/S in %s." % \
              (count_metadata, count_metadata/omega, parse_time(delta))

#        break #usefull for debugging

    return count_metadata


#Proccess the inventory of the files specified.  This is the main source
# function where all of the magic starts.
#Takes the full filepath name to the volume file in the first parameter.
#Takes the full filepath name to the metadata file in the second parameter.
#Takes the full path to the ouput directory in the third parameter.
# If output_dir is set to /dev/stdout/ then everything is sent to standard out.
def inventory2(volume_file, metadata_file, output_dir, tmp_dir, volume):
    volume_sums = {}   #The summation of all of the file sizes on a volume.
    volumes_allocated = {} #Stats on usage of storage groups.

    volume_list = read_db(os.path.split(volume_file))
    if volume_list == 1:
        print "Database " + volume_file + " read in unsuccessfully."

    #If the user entered a specific volume to check on the command line.
    if volume:
        for vol in volume_list:
            if vol['external_label'] == volume:
                volume_list = [vol]

    #Process the tapes authorized file for the VOLUME_QUATAS page.
    authorized_tapes = get_authorized_tapes()

    #Sort all of the data in data_file into the correct temporary files.
    count_metadata = sort_inventory(metadata_file, volume_list, tmp_dir)

    #print information to a real file.
    for volume in volume_list:
        if string.find(output_dir, "/dev/stdout") != -1:
            fd_output = 1
        else:
            fd_output = os.open(output_dir + volume['external_label'],
                                os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0666)
        fd_tmp = os.open(tmp_dir + volume['external_label'],
                            os.O_RDONLY, 0666)
        
        print_header(volume, fd_output) #Print the header information.
        
        #Prints the data to the appropriot output file.  Also, returns a
        # list of the lines in the tmp file, which is used to calculate
        # some statistics.
        file_data = print_data(volume, fd_tmp, fd_output)
        
        print_footer(volume, fd_output) #Print the footer information.

        #Close the file descriptors, or else will open to many and crash.
        if fd_output != 1:
            os.close(fd_output)
        os.close(fd_tmp)
        
        #Verifies the amount of data stored on the volumes.  Each call to
        # this function adds an entry into volume_sums.  The data generated
        # will be outputed by the print_volume_size_stats() funciton.
        verify_volume_sizes(file_data, volume, volume_sums)
        
        #Verifies the amount of data stored in storage groups.  Each call to
        # this function adds an entry into volumes_allocated.  The data
        # generated will be outputed by the print_volume_quotas_stats()
        # funciton.
        verify_volume_quotas(file_data, volume, volumes_allocated)


    last_access_file, volume_size_file, volumes_defined_file, \
		      volume_quotas_file, volume_quotas_format_file, \
		      total_bytes_file, volumes_too_many_mounts_file \
                      = get_vol_filenames(output_dir)

    #Create files that hold statistical data.
    print_last_access_status(volume_list, last_access_file)
    print_volume_size_stats(volume_sums, volume_list, volume_size_file)
    print_volumes_defind_status(volume_list, volumes_defined_file)
    print_volume_quotas_status(volumes_allocated, authorized_tapes,
                               volume_quotas_file)
    print_volume_quota_sums(volumes_allocated, authorized_tapes,
                            volume_quotas_file, volume_quotas_format_file)
    print_total_bytes_on_tape(volume_sums, total_bytes_file)

    return len(volume_list), count_metadata

#Proccess the inventory of the files specified.  This is the main source
# function where all of the magic starts.
#Takes the full filepath name to the volume file in the first parameter.
#Takes the full filepath name to the metadata file in the second parameter.
#Takes the full path to the ouput directory in the third parameter.
# If output_dir is set to /dev/stdout/ then everything is sent to standard out.
def inventory(volume_file, metadata_file, output_dir, cache_dir, volume):
    # determine the output path
    last_access_file, volume_size_file, volumes_defined_file, \
		      volume_quotas_file, volume_quotas_format_file, \
		      total_bytes_file, volumes_too_many_mounts_file \
                      = get_vol_filenames(output_dir)

    # open volume_summary_cache
    volume_summary_cache_file = os.path.join(cache_dir, 'volume_summary')

    if os.access(volume_summary_cache_file, os.F_OK):
        sum_f = open(volume_summary_cache_file)
        vol_sum = cPickle.load(sum_f)
        sum_f.close()
    else:
        vol_sum = {}

    vols = db.DbTable('volume', os.path.split(volume_file)[0], '/tmp', [], 0)
    files = db.DbTable('file', os.path.split(metadata_file)[0], '/tmp', ['external_label'], 0)

    n_vols = 0L
    n_files = 0L
    volume_sums = {}   #The summation of all of the file sizes on a volume.
    volumes_allocated = {} #Stats on usage of storage groups.

    t_now = time.time()
    two_day_ago = t_now - 60*60*24*2

    if string.find(output_dir, "/dev/stdout") != -1:
        fd_output = 1
    else:
        fd_output = 0

    # open file handles for statistics
    la_file = open(last_access_file, "w")
    vs_file = open(volume_size_file, "w")
    vd_file = open(volumes_defined_file, "w")
    tm_file = open(volumes_too_many_mounts_file, "w")

    vs_file.write("%10s %9s %9s %11s %9s %9s %9s %8s %8s %8s %s\n" % ("Label",
        "Actual", "Deleted", "Non-deleted", "Capacity", "Remaining",
        "Expected", "active", "deleted", "unknown",
        "Volume-Family"))

    vd_file.write("<html><pre>\n")
    vd_file.write("Date this listing was generated: %s\n" % \
        (time.ctime(time.time())))

    vd_format = "%-10s %-10s %-25s %-20s %-12s %6s %-40s\n\n"
    vd_file.write(vd_format % \
        ("label", "avail.", "system_inhibit", "user_inhibit",
         "library", "mounts",  "volume_family"))

    #Process the tapes authorized file for the VOLUME_QUATAS page.
    authorized_tapes = get_authorized_tapes()

    # get quotas

    csc = configuration_client.ConfigurationClient()
    quotas = csc.get('quotas',timeout=15,retry=3)

    unchanged = []

    n_unchanged = 0
    n_changed = 0

    # read volume ... one by one

    vc = vols.newCursor()
    vk, vv = vc.first()
    while vk:
        # skipping deleted volumes
        if vk[-8:] == ".deleted":    # skip
            vk, vv = vc.next()
            continue

        print 'processing', vk, '...',

        if vol_sum.has_key(vk):
            try:
                vsum = vol_sum[vk]
            except:
                # can be ignored
                print "(warning) cache problem ...",
                vsum = {}
        else:
            vsum = {}

        if vv.has_key('sum_mounts'):
            mounts = vv['sum_mounts']
        else:
            mounts = -1
        if vsum and vsum['last'] == vv['last_access']:
            # good, don't do anything
            active = vsum['active']
            deleted = vsum['deleted']
            unknown = vsum['unknown']
            active_size = vsum['active_size']
            deleted_size = vsum['deleted_size']
            unknown_size = vsum['unknown_size']
            total = active + deleted + unknown
            unchanged.append(vk)
            n_unchanged = n_unchanged + 1
        else:
            if fd_output != 1:
                fd_output = os.open(output_dir + vv['external_label'],
                                    os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
                                    0666)
            print_header(vv, fd_output)
    
            # some volume statistics
    
            active = 0L
            deleted = 0L
            unknown = 0L
            active_size = 0L
            deleted_size = 0L
            unknown_size = 0L
    
            # dealing with files
    
            fc = files.inx['external_label'].cursor()
            fk, pfk = fc.set(vk)
            while fk:
                # to work around the infamous missing key error due to
                # live backup
                try:
                    f = files[pfk]
                except:
                    fk, pfk = fc.nextDup()
                    continue
                if f.has_key('deleted'):
                    if f['deleted'] == 'yes':
                        deleted = deleted + 1
                        deleted_size = deleted_size + f['size']
                    else:
                        active = active + 1
                        active_size = active_size + f['size']
                else:
                    unknown = unknown + 1
                    unknown_size = unknown_size + f['size']
    
                # write out file information
                os.write(fd_output, "%10s %15s %15s %22s %7s %s\n" % \
                    (f.get('external_label', "unknown"),
                     f.get('bfid', "unknown"),
                     f.get('size', "unknown"),
                     f.get('location_cookie', "unknown"),
                     f.get('deleted', "unknown"),
                     f.get('pnfs_name0', "unknown")))
    
                n_files = n_files + 1
                fk, pfk = fc.nextDup()
            fc.close()
            total = active+deleted+unknown
            total_size = active_size+deleted_size+unknown_size
            os.write(fd_output, '\n\n%d/%d/%d/%d (active/deleted/unknown/total) files\n'% \
                     (active, deleted, unknown, total))
            os.write(fd_output, '%d/%d/%d/%d (active/deleted/unknown/total) bytes\n\n'% \
                     (active_size, deleted_size, unknown_size,
                      total_size))
            print_footer(vv, fd_output)
            if fd_output != 1:
                os.close(fd_output)
            vsum = {'last':vv['last_access'], 'active':active,
                    'deleted':deleted, 'unknown':unknown,
                    'active_size':active_size,
                    'deleted_size':deleted_size,
                    'unknown_size':unknown_size}
            vol_sum[vk] = vsum
            n_changed = n_changed + 1


        # volume_sums[vk] = {'active':active, 'deleted':deleted,
        #                    'active_size':active_size,
        #                    'deleted_size':deleted_size}
        volume_sums[vk] = (active_size+deleted_size, deleted_size,
                           active_size)

        # check quota stuff

        storage_group = string.split(vv['volume_family'], '.')[0]
        library = vv['library']
        try:
            quota = quotas['libraries'][library][storage_group]
        except:
            quota = 'N/A'

        # for the list stuff
        if total:
            written_vol = 1
            blank_vol = 0
        else:
            written_vol = 0
            blank_vol = 1

        if vv['system_inhibit'][0] == "DELETED":
            deleted_vol = 1
        else:
            deleted_vol = 0

        if volumes_allocated.has_key((library, storage_group)):
            # This is a shallow copy
            v_info = volumes_allocated[(library, storage_group)]
            volumes_allocated[(library, storage_group)] = (
                library,
                storage_group,
                v_info[2],     #quota
                v_info[3] + 1, #volume allocated
                v_info[4] + blank_vol,
                v_info[5] + written_vol,
                v_info[6] + deleted_vol,
                v_info[7] + vv['capacity_bytes'] - vv['remaining_bytes'],
                v_info[8] + active,
                v_info[9] + deleted,
                v_info[10] + unknown)
        else:
            volumes_allocated[(library, storage_group)] = (
                library,
                storage_group,
                quota,    #quota
                1, #volume allocated
                blank_vol,
                written_vol,
                deleted_vol,
                vv['capacity_bytes'] - vv['remaining_bytes'],
                active,
                deleted,
                unknown)

        # statistics stuff
        la_file.write("%f, %s %s\n" % (vv['last_access'],
                time.ctime(vv['last_access']), vv['external_label']))

        key = vv['external_label']
        format_tuple = (key,) + \
                format_storage_size(volume_sums[key][0]) + \
                format_storage_size(volume_sums[key][1]) + \
                format_storage_size(volume_sums[key][2]) + \
                format_storage_size(vv['capacity_bytes']) + \
                format_storage_size(vv['remaining_bytes']) + \
                format_storage_size(vv['capacity_bytes'] -
                                    vv['remaining_bytes']) + \
                (active, deleted, unknown, vv['volume_family'])
                
        format_string = \
           "%10s %7.2f%-2s %7.2f%-2s %9.2f%-2s %7.2f%-2s %7.2f%-2s %7.2f%-2s %8d %8d %8d %-s\n"
        
        vs_file.write(format_string % format_tuple)
        
        formated_size = format_storage_size(vv['remaining_bytes'])

        # handle mounts -- need more work
        mnts = "%6d"%(mounts)

        if mount_limit.has_key(vv['media_type']):
            if mounts > mount_limit[vv['media_type']][0]:
                if mounts <= mount_limit[vv['media_type']][1]:
                    msg = '%s (%s) exceeds %d mounts'%(
                           vv['external_label'], vv['media_type'],
                           mount_limit[vv['media_type']][0])
                    acc.alarm(e_errors.ERROR, 'Too many mounts', msg)
                mnts = '<font color=#FF0000>'+mnts+'</font>'
                # record it in tape mount file
                tm_file.write("%-10s %8.2f%2s (%-14s %8s) (%-8s  %8s) %-12s %6d %-40s\n" % \
                   (vv['external_label'],
                    formated_size[0], formated_size[1],
                    vv['system_inhibit'][0],
                    vv['system_inhibit'][1],
                    vv['user_inhibit'][0],
                    vv['user_inhibit'][1],
                    vv['library'],
                    mounts,
                    vv['volume_family']))
            if mounts >= mount_limit[vv['media_type']][1]:
                mnts = '<blink>'+mnts+'</blink>'
                msg = '<font color=#FF0000>%s (%s) exceeds %d mounts</font>'%(
                      vv['external_label'], vv['media_type'],
                      mount_limit[vv['media_type']][1])
                acc.alarm(e_errors.ERROR, 'Too many mounts', msg)
        vd_file.write("%-10s %8.2f%2s (%-14s %8s) (%-8s  %8s) %-12s %6s %-40s\n" % \
               (vv['external_label'],
                formated_size[0], formated_size[1],
                vv['system_inhibit'][0],
                vv['system_inhibit'][1],
                vv['user_inhibit'][0],
                vv['user_inhibit'][1],
                vv['library'],
                mnts,
                vv['volume_family']))

        n_vols = n_vols + 1
        print 'done'
        vk, vv = vc.next()

    vc.close()
    # dump vol_sum
    sum_f = open(volume_summary_cache_file, 'w')
    cPickle.dump(vol_sum, sum_f)
    sum_f.close()
    la_file.close()
    vs_file.close()
    vd_file.write("</pre></html>\n")
    vd_file.close()
    tm_file.close()
    # make a html copy
    os.system('cp '+volumes_defined_file+' '+volumes_defined_file+'.html')
    os.system('sed -e "s/<font color=#FF0000>//g; s/<\/font>//g; s/<blink>//g; s/<\/blink>//g" '+volumes_defined_file+'.html > '+volumes_defined_file)
    vols.close()
    files.close()

    #Create files that hold statistical data.
    print_volume_quotas_status(volumes_allocated, authorized_tapes,
                               volume_quotas_file)
    print_volume_quota_sums(volumes_allocated, authorized_tapes,
                            volume_quotas_file, volume_quotas_format_file)
    print_total_bytes_on_tape(volume_sums, total_bytes_file)

    return n_vols, n_files, n_unchanged, n_changed



def inventory_dirs():
    csc = configuration_client.ConfigurationClient()
    inven = csc.get('inventory',timeout=15,retry=3)
    checkBackedUpDatabases.check_ticket('Configuration Server', inven)
    
    inventory_dir = inven.get('inventory_dir','MISSING')
    inventory_tmp_dir = inven.get('inventory_tmp_dir','MISSING')
    inventory_extract_dir = inven.get('inventory_extract_dir','MISSING')
    inventory_rcp_dir = inven.get('inventory_rcp_dir','MISSING')
    inventory_cache_dir = inven.get('inventory_cache_dir', '/tmp')

    if inventory_dir == "MISSING":
        print "Error unable to find configdict entry inventory_dir."
        sys.exit(1)
    if inventory_tmp_dir == "MISSING":
        print "Error unable to find configdict entry inventory_tmp_dir."
        sys.exit(1)
    if inventory_extract_dir == "MISSING":
        print "Error unable to find configdict entry inventory_extract_dir."
        sys.exit(1)
    if inventory_rcp_dir == "MISSING":
        inventory_rcp_dir = '' #Set this to the empty string.

    return inventory_dir, inventory_tmp_dir, inventory_extract_dir, \
           inventory_rcp_dir, inventory_cache_dir


if __name__ == "__main__":
    #Don't bother with initialization if they only want help
    if "--help" in sys.argv:
        inventory_usage()
        sys.exit(0)

    alarm_client.Trace.init('INVENTORY')

    csc = configuration_client.ConfigurationClient()
    mount_limit = csc.get('tape_mount_limit', timeout=15,retry=3)
    acc =alarm_client.AlarmClient(csc)
    if mount_limit['status'][0] == e_errors.OK:
        del mount_limit['status']
    else:
        mount_limit = {}

    #Retrieve the necessary directories from the enstore servers.
    # Extract_dir is ignored by inventory.py.
    (backup_dir, extract_dir, current_dir, backup_node) = \
                 checkBackedUpDatabases.configure()
    (inventory_dir, inventory_tmp_dir, inventory_extract_dir,
     inventory_rcp_dir, inventory_cache_dir) = inventory_dirs()

    #Make sure all of the directories end with a /
    if backup_dir[-1] != "/": backup_dir = backup_dir + "/"
    if current_dir[-1] != "/": current_dir = current_dir + "/"
    if inventory_dir[-1] != "/": inventory_dir = inventory_dir + "/"
    if inventory_tmp_dir[-1] != "/":
        inventory_tmp_dir = inventory_tmp_dir + "/"
    if inventory_extract_dir[-1] != "/":
        inventory_extract_dir = inventory_extract_dir + "/"
    if inventory_rcp_dir != "" and inventory_rcp_dir[-1] != "/":
        inventory_rcp_dir = inventory_rcp_dir + "/"
        
#    print "backup_dir", backup_dir
#    print "current_dir", current_dir
#    print "inventory_dir", inventory_dir
#    print "inventory_tmp_dir", inventory_tmp_dir
#    print "inventory_extract_dir", inventory_extract_dir
#    print "inventory_rcp_dir", inventory_rcp_dir
#    print "extract_dir", extract_dir

    #Look through the arguments list for valid arguments.
    if "--stdout" in sys.argv:
        output_dir = "/dev/stdout/"
        inventory_rcp_dir = "" #Makes no sense to move files that don't exist.
    elif "-o" in sys.argv:
        output_dir = sys.argv[sys.argv.index("-o") + 1] + "/"
    else:
        output_dir = inventory_dir
        
    if "-f" in sys.argv:
        file_file = sys.argv[sys.argv.index("-f") + 1]
    else:
        file_file = inventory_extract_dir + "file"
        
    if "-v" in sys.argv:
        volume_file = sys.argv[sys.argv.index("-v") + 1]
    else:
        volume_file = inventory_extract_dir + "volume"

    if "--volume" in sys.argv:
        volume = sys.argv[sys.argv.index("--volume") + 1]
        output_dir = "/dev/stdout/"
        inventory_rcp_dir = "" #Makes no sense to move files that don't exist.
    else:
        volume = None

    #Remove the contents of existing direcories and create them if they do
    # not exist.
    create_clean_dirs(output_dir, inventory_extract_dir, inventory_tmp_dir)

    #If the backup needs to be extracted (the defualt) then do.
    if "-f" not in sys.argv and "-v" not in sys.argv:
        container = checkBackedUpDatabases.check_backup(backup_dir,backup_node)
        checkBackedUpDatabases.extract_backup(inventory_extract_dir, container)

    #Inventory is the main function that does work.
    counts = inventory(volume_file, file_file, output_dir,
                       inventory_cache_dir, volume)
    
    #Cleanup those directories that we don't care about its contents.
    cleanup_dirs(inventory_tmp_dir, inventory_extract_dir)
    checkBackedUpDatabases.clean_up(current_dir) #Simple "cleanup".

    #Move all of the output files over to the web server node.
    if inventory_rcp_dir:
        if string.find(output_dir, "/dev/stdout") == -1:
            print "enrcp %s %s" % (output_dir + "*", inventory_rcp_dir)
            os.system("enrcp %s %s" % (output_dir + "*", inventory_rcp_dir))

    #Print stats regarding the data generated.
    delta_t = time.time() - t0
    print "%d files on %d volumes processed in %s." % \
          (counts[1], counts[0], parse_time(delta_t))
    print "%d volumes changed while %d volume unchanged." % \
          (counts[3], counts[2])

