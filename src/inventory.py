#!/usr/bin/env python

#system imports
import sys
import time
import string
import os
import tempfile
import pprint
import cPickle
import shutil

#user imports
import edb
import configuration_client
import enstore_constants
import alarm_client
import e_errors
import quota as equota

mount_limit = {}
acc = None

#Grab the start time.
t0 = time.time()

def get_vq_format_file(output_dir):
    return os.path.join(output_dir, enstore_constants.VQFORMATED)

def tod():
    return time.strftime("%c",time.localtime(time.time()))

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
        declaration_error = "/dev/stdout"
    else:
        last_access_file = os.path.join(output_dir, "LAST_ACCESS")
        volume_size_file = os.path.join(output_dir, "VOLUME_SIZE")
        volumes_defined_file = os.path.join(output_dir, "VOLUMES_DEFINED")
        volumes_too_many_mounts_file = os.path.join(output_dir, "VOLUMES_TOO_MANY_MOUNTS")
        volume_quotas_file = os.path.join(output_dir, "VOLUME_QUOTAS")
	volume_quotas_format_file = get_vq_format_file(output_dir)
        total_bytes_file = os.path.join(output_dir, "TOTAL_BYTES_ON_TAPE")
        declaration_error = os.path.join(output_dir, "DECLARATION_ERROR")
    return last_access_file, volume_size_file, volumes_defined_file, \
		      volume_quotas_file, volume_quotas_format_file, \
		      total_bytes_file, volumes_too_many_mounts_file, \
                      declaration_error

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
    print "Usage: " + sys.argv[0],
    print "[[-o output_directory] | [-stdout]] [--help]"
    print "   -o=      set the output directory"
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
        volume_size = volume_size / 1024.0
        count = count + 1


    return volume_size, suffix[count]

# remove_files(file_list, dir)
def remove_files(files, dir):
    for i in files:
        p = os.path.join(dir, i)
        if os.path.isdir(p):
            shutil.rmtree(p)
        else:
            os.remove(p)

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
                
            remove_files(os.listdir(dir),dir)

#Read in the information from the authorized tapes file.
def get_authorized_tapes2():
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
    
def print_volume_quotas_status(volume_quotas, authorized_tapes, output_file, quotas):

    order = quotas.get('order', {})
    vq_file = open(output_file, "w")

    vq_file.write("Date this listing was generated: %s\n" % \
                  time.asctime(time.localtime(time.time())))
    
    vq_file.write("   %-15s %-15s %-11s %-12s %-6s %-9s %-10s %-12s %-7s %12s %-12s %-13s %s\n" %
          ("Library", "Storage Group", "Req. Alloc.",
           "Auth. Alloc.", "Quota", "Allocated",
           "Blank Vols", "Used Vols", "Deleted Vols", "Space Used ",
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
            file_string = os.path.join(output_dir, volume['external_label'])

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
        
    

def is_b_library(lib):
    if lib == 'eval-b' or lib[-5:] == '9940B' or lib[-9:] == 'Migration':
        return 1
    return 0

#Proccess the inventory of the files specified.  This is the main source
# function where all of the magic starts.
#Takes the full filepath name to the volume file in the first parameter.
#Takes the full filepath name to the metadata file in the second parameter.
#Takes the full path to the ouput directory in the third parameter.
# If output_dir is set to /dev/stdout/ then everything is sent to standard out.
def inventory(output_dir, cache_dir):
    # determine the output path
    last_access_file, volume_size_file, volumes_defined_file, \
		      volume_quotas_file, volume_quotas_format_file, \
		      total_bytes_file, volumes_too_many_mounts_file, \
                      declaration_error \
                      = get_vol_filenames(output_dir)

    # open volume_summary_cache
    volume_summary_cache_file = os.path.join(cache_dir, 'volume_summary')

    if os.access(volume_summary_cache_file, os.F_OK):
        sum_f = open(volume_summary_cache_file)
        vol_sum = cPickle.load(sum_f)
        sum_f.close()
    else:
        vol_sum = {}

    csc = configuration_client.ConfigurationClient()
    dbinfo = csc.get('database')

    vols = edb.VolumeDB(host=dbinfo['db_host'], jou='/tmp')
    file = edb.FileDB(host=dbinfo['db_host'], jou='/tmp', rdb = vols.db)
    eq = equota.Quota(vols.db)

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
    de_file = open(declaration_error, "w")

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

    de_file.write("Date this listing was generated: %s\n\n"%(
        time.ctime(time.time())))
    de_file.write("\t%12s\t%12s\t%12s\t%12s\t%12s\n\n"%("volume", "actual size", "capacity", "library", "media type"))

    de_format = "%6d\t%12s\t%12d\t%12d\t%12s\t%12s\t%s\n"
    de_count = 0

    #Process the tapes authorized file for the VOLUME_QUATAS page.
    authorized_tapes = eq.get_authorized_tapes()

    # get quotas

    # csc = configuration_client.ConfigurationClient()
    # quotas = csc.get('quotas',timeout=15,retry=3)
    quotas = eq.quota_enabled()
    if quotas == None:
        quotas = {}

    unchanged = []

    n_unchanged = 0
    n_changed = 0

    # read volume ... one by one

    for vk in vols.keys():
        vv = vols[vk]
        # skipping deleted volumes
        try:
            if vk[-8:] == ".deleted" or vv['external_label'][-8:] == ".deleted":    # skip
                continue
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            print exc_type, exc_value
            print "vk =", `vk`
            print "vv =", `vv`
            sys.exit(1)

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
        if vsum and long(vsum['last']) == long(vv['last_access']):
            # good, don't do anything
            active = vsum['active']
            deleted = vsum['deleted']
            unknown = vsum['unknown']
            active_size = vsum['active_size']
            deleted_size = vsum['deleted_size']
            unknown_size = vsum['unknown_size']
            total = active + deleted + unknown
            total_size = active_size+deleted_size+unknown_size
            unchanged.append(vk)
            n_unchanged = n_unchanged + 1
        else:
            if fd_output != 1:
                fd_output = os.open(os.path.join(output_dir, vv['external_label']),
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

            # get all bfids of the volume

            q = "select bfid from file, volume\
                 where volume.label = '%s' and \
                     file.volume = volume.id;"%(vk)
            res = file.db.query(q).getresult()
            bfids = []
            for i in res:
                bfids.append(i[0])
            for pfk in bfids:
                # to work around the infamous missing key error due to
                # live backup
                f = file[pfk]
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

        # check if the volume is declared right
        if vk[:3] != 'CLN':
            actual_size = total_size+vv['remaining_bytes']
            if total_size <= 0:
                remark = 'never written'
            else:
                remark = ''
            if vv['media_type'] == '9940':
                if actual_size > 80*1048576*1024 or is_b_library(vv['library']):
                    de_count = de_count + 1
                    de_file.write(de_format%(de_count, vk, actual_size, vv['capacity_bytes'], vv['library'], vv['media_type'], remark))
            elif vv['media_type'] == '9940B':
                if actual_size and (actual_size < 100*1048576*1024 or not is_b_library(vv['library']) or vv['capacity_bytes'] < 180*1048576*1024):
                    de_count = de_count + 1
                    de_file.write(de_format%(de_count, vk, actual_size, vv['capacity_bytes'], vv['library'], vv['media_type'], remark))
            elif is_b_library(vv['library']) and vv['media_type'] != '9940B':
                    de_count = de_count + 1
                    de_file.write(de_format%(de_count, vk, actual_size, vv['capacity_bytes'], vv['library'], vv['media_type'], remark))

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
        if not total and vv['volume_family'][-10:] == '.none.none':
            written_vol = 0
            blank_vol = 1
        else:
            written_vol = 1
            blank_vol = 0

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
                v_info[7] + total_size,
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
                total_size,
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

    # dump vol_sum
    sum_f = open(volume_summary_cache_file, 'w')
    cPickle.dump(vol_sum, sum_f)
    sum_f.close()
    la_file.close()
    vs_file.close()
    vd_file.write("</pre></html>\n")
    vd_file.close()
    tm_file.close()
    de_file.close()
    # make a html copy
    os.system('cp '+volumes_defined_file+' '+volumes_defined_file+'.html')
    os.system('sed -e "s/<font color=#FF0000>//g; s/<\/font>//g; s/<blink>//g; s/<\/blink>//g" '+volumes_defined_file+'.html > '+volumes_defined_file)
    vols.close()
    file.close()

    #Create files that hold statistical data.
    print_volume_quotas_status(volumes_allocated, authorized_tapes,
                               volume_quotas_file, quotas)
    print_volume_quota_sums(volumes_allocated, authorized_tapes,
                            volume_quotas_file, volume_quotas_format_file)
    print_total_bytes_on_tape(volume_sums, total_bytes_file)

    return n_vols, n_files, n_unchanged, n_changed

def inventory_dirs():
    csc = configuration_client.ConfigurationClient()
    inven = csc.get('inventory',timeout=15,retry=3)
    if not 'status' in inven.keys():
        print tod(), 'Configuration Server NOT RESPONDING'
        sys.exit(1)
    elif inven['status'][0] != e_errors.OK:
        print tod(), 'Configuration Server BAD STATUS', `inven['status']`
        sys.exit(1)
    else:
        print tod(), 'Configuration Server ok'
    
    inventory_dir = inven.get('inventory_dir','MISSING')
    inventory_rcp_dir = inven.get('inventory_rcp_dir','MISSING')
    inventory_cache_dir = inven.get('inventory_cache_dir', '/tmp')

    if inventory_dir == "MISSING":
        print "Error unable to find configdict entry inventory_dir."
        sys.exit(1)
    if inventory_rcp_dir == "MISSING":
        inventory_rcp_dir = '' #Set this to the empty string.

    return inventory_dir, inventory_rcp_dir, inventory_cache_dir


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

    (inventory_dir, inventory_rcp_dir, inventory_cache_dir) = inventory_dirs()

#    print "inventory_dir", inventory_dir
#    print "inventory_rcp_dir", inventory_rcp_dir

    #Look through the arguments list for valid arguments.
    if "--stdout" in sys.argv:
        output_dir = "/dev/stdout/"
        inventory_rcp_dir = "" #Makes no sense to move files that don't exist.
    elif "-o" in sys.argv:
        output_dir = sys.argv[sys.argv.index("-o") + 1]
    else:
        output_dir = inventory_dir
        
    #Remove the contents of existing direcories and create them if they do
    # not exist.
    create_clean_dirs(output_dir)

    #Inventory is the main function that does work.
    counts = inventory(output_dir, inventory_cache_dir)
    
    #Move all of the output files over to the web server node.
    if inventory_rcp_dir:
        if string.find(output_dir, "/dev/stdout") == -1:
            print "enrcp %s %s" % (os.path.join(output_dir, "*"), inventory_rcp_dir)
            os.system("enrcp %s %s" % (os.path.join(output_dir, "*"), inventory_rcp_dir))

    #Print stats regarding the data generated.
    delta_t = time.time() - t0
    print "%d files on %d volumes processed in %s." % \
          (counts[1], counts[0], parse_time(delta_t))
    print "%d volumes changed while %d volume unchanged." % \
          (counts[3], counts[2])

