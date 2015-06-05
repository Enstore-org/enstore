#!/usr/bin/env python
###############################################################################
#
# $Id$
#
###############################################################################

#system imports
import sys
import time
import string
import os
import cPickle
import shutil

#user imports
import edb
import configuration_client
import enstore_constants
import alarm_client
import e_errors
import quota as equota
import accounting
import dbaccess
import write_protection_alert

#Multiplier to determine if the actual size of the tape is close enough
# to the the stated capacity of that media type.
CAPACITY_TOLERANCE = .05

mount_limit = {}
acc = None

#Grab the start time.
t0 = time.time()

def get_vq_format_file(output_dir):
    return os.path.join(output_dir, enstore_constants.VQFORMATED)

def tod():
    return time.strftime("%c",time.localtime(time.time()))

# time2timestamp(t) -- convert time to "YYYY-MM-DD HH:MM:SS"
def time2timestamp(t):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t))

# return filenames
def get_vol_filenames(output_dir):
    if string.find(output_dir, "/dev/stdout") != -1:
        last_access_file = "/dev/stdout"
        volume_size_file = "/dev/stdout"
        volumes_defined_file = "/dev/stdout"
        write_protect_alert_file = "/dev/stdout"
        volumes_too_many_mounts_file = "/dev/stdout"
        volume_quotas_file = "/dev/stdout"
	volume_quotas_format_file = "/dev/stdout"
        total_bytes_file = "/dev/stdout"
        declaration_error = "/dev/stdout"
        migrated_volumes = "/dev/stdout"
        duplicated_volumes = "/dev/stdout"
        recyclable_volumes = "/dev/stdout"
    else:
        last_access_file = os.path.join(output_dir, "LAST_ACCESS")
        volume_size_file = os.path.join(output_dir, "VOLUME_SIZE")
        volumes_defined_file = os.path.join(output_dir, "VOLUMES_DEFINED")
        write_protect_alert_file = os.path.join(output_dir, "WRITE_PROTECTION_ALERT")
        volumes_too_many_mounts_file = os.path.join(output_dir, "VOLUMES_TOO_MANY_MOUNTS")
        volume_quotas_file = os.path.join(output_dir, "VOLUME_QUOTAS")
	volume_quotas_format_file = get_vq_format_file(output_dir)
        total_bytes_file = os.path.join(output_dir, "TOTAL_BYTES_ON_TAPE")
        declaration_error = os.path.join(output_dir, "DECLARATION_ERROR")
        migrated_volumes = os.path.join(output_dir, "MIGRATED_VOLUMES")
        duplicated_volumes = os.path.join(output_dir, "DUPLICATED_VOLUMES")
        recyclable_volumes = os.path.join(output_dir, "RECYCLABLE_VOLUMES")
    return last_access_file, volume_size_file, volumes_defined_file, \
        volume_quotas_file, volume_quotas_format_file, \
        total_bytes_file, volumes_too_many_mounts_file, \
        declaration_error, migrated_volumes, duplicated_volumes, \
        recyclable_volumes, write_protect_alert_file

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
    for dname in dirs:
        #An empty output directory would be nice.
        if string.find(dname, "/dev/stdout") == -1:
            try:
                os.stat(dname)
            except OSError:
                os.mkdir(dname, 0755)

            remove_files(os.listdir(dname), dname)

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
    if one['modification_time'] < two['modification_time']:
        return -1
    elif one['modification_time'] > two['modification_time']:
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

    if volume['modification_time'] == -1:
        os.write(fd, "Last accessed on: Never\n")
    else:
        out_string = "Last accessed on: " + \
              time.asctime(time.localtime(volume['modification_time'])) + "\n"
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
# volume: from vcc.inq_vol()
# s_i: Summary Information.  There should be 8 keys in this dictionary.
#      for active, deleted, unknown and total files; and active_size,
#      deleted_size, unknown_size, and total_size in bytes.
def print_footer(volume, s_i, fd):

    #Write out the summary information first.
    os.write(fd, '\n\n%d/%d/%d/%d (active/deleted/unknown/total) files\n'% \
             (s_i['active'], s_i['deleted'], s_i['unknown'], s_i['total']))
    os.write(fd, '%d/%d/%d/%d (active/deleted/unknown/total) bytes\n\n'% \
             (s_i['active_size'], s_i['deleted_size'], s_i['unknown_size'],
              s_i['total_size']))

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
                      % (volume['modification_time'],
                         time.asctime(time.localtime(volume['modification_time'])),
                         volume['external_label']))
    la_file.close()

def print_volumes_defined_status(volume_list, output_file):
    volume_list.sort(el_sort)
    vd_file = open(output_file, "w")


    vd_file.write("Date this listing was generated: %s\n\n" % \
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

    vq_file.write("Date this listing was generated: %s\n\n" % \
                  time.asctime(time.localtime(time.time())))

    fields = ("    ", "Library", "Storage Group", "Requested",
              "Authorized", "Quota", "Allocated",
              "Blank", "Used", "Deleted", "Space Used",
              "Active", "Deleted", "Unknown",
              "Recyclable", "Migrated", "Duplicated")
    fields2 = ("","","","","","","","","","","","Files","Files","Files","","", "")

    fw = []
    for i in fields:
        fw.append(len(i))

    for key in volume_quotas.keys():
        fw[1] = max(fw[1], len(volume_quotas[key][0]))
        # remember none: emergency
        fw[2] = max(fw[2], len(volume_quotas[key][1]), 15)
        at = authorized_tapes.get(volume_quotas[key][:2], ("N/A", "N/A"))
        if volume_quotas[key][4]:
            bk = "%d/%d"%(volume_quotas[key][13], volume_quotas[key][4])
        else:
            bk = '0'
        fw[3] = max(fw[3], len(str(at[0])))
        fw[4] = max(fw[4], len(str(at[1])))
        fw[5] = max(fw[5], len(str(volume_quotas[key][2])))
        fw[6] = max(fw[6], len(str(volume_quotas[key][3])))
        fw[7] = max(fw[7], len(bk))
        fw[8] = max(fw[8], len(str(volume_quotas[key][5])))
        fw[9] = max(fw[9], len(str(volume_quotas[key][6])))
        qs = format_storage_size(volume_quotas[key][7])
        fw[10] = max(fw[10], len(str(int(qs[0])))+3+len(qs[1]))
        fw[11] = max(fw[11], len(str(volume_quotas[key][8])))
        fw[12] = max(fw[12], len(str(volume_quotas[key][9])))
        fw[13] = max(fw[13], len(str(volume_quotas[key][10])))
        fw[14] = max(fw[14], len(str(volume_quotas[key][11])))
        fw[15] = max(fw[15], len(str(volume_quotas[key][12])))
        fw[16] = max(fw[16], len(str(volume_quotas[key][14])))

    tl = 0
    for i in fw:
        tl = tl + i

    header_format = "%%%ds  %%-%ds  %%-%ds  %%%ds  %%%ds  %%%ds  %%%ds  %%%ds  %%%ds  %%%ds  %%%ds  %%%ds  %%%ds  %%%ds  %%%ds  %%%ds  %%%ds\n"%tuple(fw)
    # take care of formated size
    fw[10] = fw[10]-2
    fw.insert(11, 2)
    row_format = "%%%dd  %%-%ds  %%-%ds  %%%ds  %%%ds  %%%ds  %%%dd  %%%ds  %%%dd  %%%dd  %%%d.2f%%%ds  %%%dd  %%%dd  %%%dd  %%%dd  %%%dd  %%%dd\n"%tuple(fw)

    vq_file.write(header_format%fields)
    vq_file.write(header_format%fields2)
    vq_file.write('\n')

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
            if volume_quotas[keys][4]:
                bk = '%d/%d'%(volume_quotas[keys][13], volume_quotas[keys][4])
            else:
                bk = '0'
            formated_tuple = (count, volume_quotas[keys][0],) + \
                             (formated_storage_group,) + \
                             authorized_tapes.get(volume_quotas[keys][:2],
                                                  ("N/A", "N/A")) + \
                             volume_quotas[keys][2:4] + \
                             (bk,) + \
                             volume_quotas[keys][5:7] + \
                             format_storage_size(volume_quotas[keys][7]) + \
                             volume_quotas[keys][8:13] + \
                             (volume_quotas[keys][14],)
            vq_file.write(row_format % formated_tuple)
        vq_file.write("\n") #insert newline between sections
    vq_file.close()
    return row_format, tl+30


def print_volume_quota_sums(volume_quotas, authorized_tapes, output_file,
			    output_format_file, fmt, tl):
    vq_file = open(output_file, "a")
    vq_file.write(("-" * tl) + "\n\n")

    vq_format_file = open(output_format_file, "w")

    #Sum up each column for each library and print the results
    library_dict = {}
    library_format_dict = {}
    requested = authorized = 0
    quotas = volume_quotas.keys()
    for key in quotas:
        #Get the current (library, storage_group) out of the dict.
        (l, sg, quota, allocated, blank_v, written_v, deleted_v, used,
            active_f, deleted_f, unknown_f, recyclable_v, migrated_v,
            wp_n, duplicated_v) = volume_quotas[key]

        TUPLE_LEN = 16
        #For each library total up the numbers.
        try: # total up the number of requested tapes.
            requested = int(authorized_tapes.get((l, sg), (0,) * 2)[0]) + \
                        int(library_dict.get(l, (0,) * TUPLE_LEN)[2])
        except ValueError:
            requested = int(library_dict.get(l, (0,) * TUPLE_LEN)[2])
        try: # total up the number of authorized tapes.
            authorized = int(authorized_tapes.get((l, sg), (0,) * 2)[1]) + \
                         int(library_dict.get(l, (0,) * TUPLE_LEN)[3])
        except ValueError:
            authorized = int(library_dict.get(l, (0,) * TUPLE_LEN)[3])
        try:
            quota = int(quota) + int(library_dict.get(l, (0,) * 15)[4])
        except:
            quota = int(library_dict.get(l, (0,) * TUPLE_LEN)[4])
        allocated = allocated + library_dict.get(l, (0,) * TUPLE_LEN)[5]
        blank_v = blank_v + library_dict.get(l, (0,) * TUPLE_LEN)[6]
        written_v =  written_v + library_dict.get(l, (0,) * TUPLE_LEN)[7]
        deleted_v =  deleted_v + library_dict.get(l, (0,) * TUPLE_LEN)[8]
        used = used + library_dict.get(l, (0,) * TUPLE_LEN)[9]
        active_f = active_f + library_dict.get(l, (0,) * TUPLE_LEN)[10]
        deleted_f =  deleted_f + library_dict.get(l, (0,) * TUPLE_LEN)[11]
        unknown_f = unknown_f + library_dict.get(l, (0,) * TUPLE_LEN)[12]
        recyclable_v = recyclable_v + library_dict.get(l, (0,) * TUPLE_LEN)[13]
        migrated_v = migrated_v + library_dict.get(l, (0,) * TUPLE_LEN)[14]
        duplicated_v = duplicated_v + library_dict.get(l, (0,) * TUPLE_LEN)[15]

        library_dict[l] = (l, "", requested, authorized, quota, allocated,
                           blank_v, written_v, deleted_v, used, active_f,
                           deleted_f, unknown_f, recyclable_v, migrated_v,
                           duplicated_v)
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
        # vq_file.write("%2d %-15s %-15s %-11s %-12s %-6s %-9d %-10d %-12d %-12d %9.2f%-3s %-12d %-13d %-13d %-16d %-13d\n"
        # vq_file.write("%2d %-15s %-15s %-11s %-12s %-6s %-9d %-10d %-12d %-12d %9.2f%-3s %-12d %-13d %-13d %-16d %-13d\n"
        vq_file.write(fmt % formated_tuple)
	vq_format_file.write("%s %s\n"%(key, library_format_dict[key]))
    vq_file.write("\n") #insert newline between sections

    blanks = authorized_tapes.get('blanks', None)
    if blanks != None:
        vq_file.write("blanks=" + blanks + "\n")

    vq_file.close()
    vq_format_file.close()

def print_total_bytes_on_tape(volume_sums, output_file):
    summation = 0
    for line in volume_sums.keys():
        summation = volume_sums[line][0] + summation

    tbot_file = open(output_file, "w")

    tbot_file.write("%.2f %s\n" % format_storage_size(summation))

    tbot_file.close()

##############################################################################
##############################################################################

def print_common_header(fp):
    command_name = os.path.basename(sys.argv[0])

    fp.write("Date this listing was generated: %s\n" % \
        (time.ctime(time.time())))
    fp.write("Brought to You by: %s\n\n" % (command_name,))

def print_last_access_header(fp):
    print_common_header(fp)

def print_last_access_footer(fp):
    pass

def print_volume_size_header(fp):
    print_common_header(fp)

    vs_format = "%10s %9s %9s %11s %9s %9s %9s %8s %8s %8s %s\n"
    vs_titles = ("Label", "Actual", "Deleted", "Non-deleted", "Capacity",
                 "Remaining", "Expected", "active", "deleted", "unknown",
                 "Volume-Family")
    fp.write(vs_format % vs_titles)

def print_volume_size_footer(fp):
    pass

def print_volumes_defined_header(fp):
    fp.write("<html><pre>\n")

    print_common_header(fp)

    vd_format = "%-10s %-10s %-25s %-20s %-12s %-3s %6s %-40s\n\n"
    vd_titles = ("label", "avail.", "system_inhibit", "user_inhibit",
                 "library", "wp", "mounts",  "volume_family")
    fp.write(vd_format % vd_titles)

def print_volumes_defined_footer(fp):
    fp.write("</pre></html>\n")
    pass

#n_vols: total number of volumes
#n_rf_vols: number of volumes that should be write protected per library
#n_not_rp_vols: number of volumes that are not write protected, but should be per library
#n_rp_vols: number of volumes that are write protected and are write protected per library
#n_vols_lib: number of volumes per library

def print_too_many_mounts_header(fp):
    print_common_header(fp)

def print_too_many_mounts_footer(fp):
    pass

def print_declaration_error_header(fp):
    print_common_header(fp)

    de_format = "\t%12s\t%12s\t%12s\t%12s\t%12s\t%s\n\n"
    de_titles = ("volume", "actual size", "capacity", "library",
                 "media type", "remark")
    fp.write(de_format % de_titles)

def print_declaration_error_footer(fp):
    pass

def print_migrated_volumes_header(fp):
    print_common_header(fp)

    fp.write("These migrated or cloned volumes MAY be recycled or deleted from system:\n\n")

def print_migrated_volumes_footer(fp, n_migrated):
    fp.write("\n(%d volumes)"%(n_migrated))

def print_duplicated_volumes_header(fp):
    print_common_header(fp)

    fp.write("These duplicated volumes can be swapped:\n\n")

def print_duplicated_volumes_footer(fp, n_duplicated):
    fp.write("\n(%d volumes)"%(n_duplicated))

def print_recyclable_volumes_header(fp):
    print_common_header(fp)

    fp.write("These volumes are full and have only deleted files.\n")
    fp.write("They MAY be recycled.\n\n")

#rc_file2 is not really an open file handle.  Instead, it is a list of lines
# to append to the end of the first rc_file.
def print_recyclable_volumes_footer(fp, n_recyclable, rc_file2,
                                    n_recyclable2, rc_contents_list3,
                                    number_of_recyclable3):

    # write out the count of recyclable volumes
    fp.write("\n(%d volumes)" % (n_recyclable))
    # write out empty spacer lines
    fp.write("\n\n\n\n")

    #Output the readonly recyclable volumes.
    fp.write("These volumes are readonly and have only deleted files.\n")
    fp.write("They probably can be recycled.\n\n")
    #Append total before writing second category out.
    rc_file2.append("\n(%d volumes)\n" % (n_recyclable2))
    for l in rc_file2:
        fp.write(l)
    # write out empty spacer lines
    fp.write("\n\n\n\n")

    #Output the migration recyclable volumes.
    fp.write("These are migrated/duplicated/cloned volumes of deleted files.\n")
    fp.write("They probably can be recycled.\n\n")
    #Append total before writing third category out.
    rc_contents_list3.append("\n(%d volumes)\n" % (number_of_recyclable3))
    for l in rc_contents_list3:
        fp.write(l)


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
    minute = int(seconds) / 60 % 60
    second = int(seconds) % 60

    out_string = ""

    if hour == 1:
        out_string = "%d hour " % (hour)
    elif hour > 0:
        out_string = "%d hours " % (hour)

    if minute == 1:
        out_string = "%s%d minute " % (out_string, minute)
    elif minute > 0:
        out_string = "%s%d minutes " % (out_string, minute)

    if second == 1:
        out_string = "%s%d second" % (out_string, second)
    elif second >= 0:
        out_string = "%s%d seconds" % (out_string, second)

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
    for file_data in volume_data:
        row = string.split(file_data)
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
    return lib.find('9940B') >= 0 or lib == 'eval-b' or lib[-9:] == 'Migration'

def is_lto3_library(lib):
    return lib.find('LTO3') >= 0


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
        declaration_error, migrated_volumes, duplicated_volumes, \
        recyclable_volumes, write_protect_alert_file \
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
    invinfo = csc.get('inventory')
    wpa_states = invinfo.get('wpa_states', [])
    wpa_excluded_media_types = ['null', 'disk']
    wpa_excluded_libraries = invinfo.get('wpa_excluded_libraries', [])
    dbinfo = csc.get('database')

    vol_db = edb.VolumeDB(host=dbinfo['db_host'],
                          user=dbinfo['dbuser'],
                          port=dbinfo.get('db_port',8888),
                          jou='/tmp',
                          max_connections=1)
    file_db = edb.FileDB(host=dbinfo['db_host'],
                         user=dbinfo['dbuser'],
                         port=dbinfo.get('db_port',8888),
                         jou='/tmp',
                         max_connections=1)
    # log to accounting db
    accinfo = csc.get(enstore_constants.ACCOUNTING_SERVER)
    acs = accounting.accDB(accinfo['dbhost'],
                           accinfo['dbname'],
                           accinfo.get("dbport",8800),
                           accinfo['dbuser'])
    #

    db = dbaccess.DatabaseAccess(maxconnections=1,
                                 host     = dbinfo.get("db_host","localhost"),
                                 database = dbinfo.get('dbname', "enstoredb"),
                                 port     = dbinfo.get('db_port', 5432),
                                 user     = dbinfo.get('dbuser', "enstore"))
    eq = equota.Quota(db)

    #Get the media_types for each library.
    library_media_types = {}
    q = "select media_type,library,count(label) from volume where system_inhibit_0 != 'DELETED' group by media_type,library order by count;"
    res = vol_db.dbaccess.query_getresult(q)
    for row in res:
        #The key is the (short) library name.  The value is the media type.
        # The SQL above sorts them by count, so if two libraries get
        # different media types, the most common one 'wins'.
        library_media_types[row[1]] = row[0]

    n_vols = 0L          # total number of volumes
    n_files = 0L         # total number of files
    n_vols_lib = {}      # number of volumes per library
    n_rf_vols = {}       # number of vols that should be write-protected per library
    n_not_rp_vols = {}   # number of volumes in n_rp_vols that are not write-protected per library
    n_rp_vols = {}       # number of volumes that are write-protected per library

    volume_sums = {}       #The summation of all of the file sizes on a volume.
    volumes_allocated = {} #Stats on usage of storage groups.

    #t_now = time.time()
    #two_day_ago = t_now - 60*60*24*2

    if string.find(output_dir, "/dev/stdout") != -1:
        fd_output = 1
    else:
        fd_output = 0

    #
    # generate WRITE_PROTECTION_ALERT
    #
    write_protection_alert.do_work(write_protect_alert_file)

    # open file handles for statistics
    la_file = open(last_access_file, "w")
    vs_file = open(volume_size_file, "w")
    vd_file = open(volumes_defined_file, "w")  #html; not text
    tm_file = open(volumes_too_many_mounts_file, "w")
    de_file = open(declaration_error, "w")
    mv_file = open(migrated_volumes, "w")
    dv_file = open(duplicated_volumes, "w")
    rc_file = open(recyclable_volumes, "w")
    rc_file2 = []
    rc_contents_list3 = []

    #Print the headers for each type of output file.
    print_last_access_header(la_file)
    print_volume_size_header(vs_file)
    print_volumes_defined_header(vd_file)
    print_too_many_mounts_header(tm_file)
    print_declaration_error_header(de_file)
    print_migrated_volumes_header(mv_file)
    print_duplicated_volumes_header(dv_file)
    print_recyclable_volumes_header(rc_file)

    #

    #Redefine de_format for printing out the lines.
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
    n_migrated = 0
    n_duplicated = 0
    n_recyclable = 0
    n_recyclable2 = 0
    number_of_recyclable3 = 0

    # produce list of (volume,count) of volumes
    # containing active package files w/o active package count

    q = """
    SELECT v.label,
       count(*)
    FROM file f,
       volume v
    WHERE v.id=f.volume
       AND f.deleted='n'
       AND f.package_id=f.bfid
       AND f.active_package_files_count=0
    GROUP BY v.label
    """

    res = file_db.query(q)
    volumes=dict(res)

    # read volume ... one by one

    for vk in vol_db.keys():
        vv = vol_db[vk]
        if not vv: # vk is gone
            vv = vol_db[vk+'.deleted']
            if not vv:
                print "%s is missing"%(vk)
                sys.exit(1)
        if vv.get('media_type') in ('null','disk') :
            continue

        # skipping deleted volumes
        try:
            if vv['external_label'][-8:] == ".deleted" \
               or vk[-8:] == ".deleted":    #skip
                continue
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            print exc_type, exc_value
            print "vk =", `vk`
            print "vv =", `vv`
            sys.exit(1)

	tt0 = time.time()
        print 'processing', vk, '...',

        if vol_sum.has_key(vk):
            try:
                vsum = vol_sum[vk]
                vsum['external_label']=vk
            except:
                # can be ignored
                print "(warning) cache problem ...",
                vsum = {}
        else:
            vsum = {}

        ###
        ### Address the pages that report on the status(es) of the volume.
        ###

        skipped = False
        active = 0
        deleted = 0
        unknown = 0
        active_size = 0
        deleted_size = 0
        unknown_size = 0
        total = 0
        total_size = 0
        # First, skip updating the file information for volumes that have
        # not been updated recently.
        if vsum and long(vsum['last']) == long(vv['modification_time']):
            skipped = True
            # subtract from active counts number of active package files
            # that have constituent files marked deleted
            active  = vsum['active'] - volumes.get(vsum['external_label'],0)
            deleted = vsum['deleted']
            unknown = vsum['unknown']
            active_size = vsum['active_size']
            deleted_size = vsum['deleted_size']
            unknown_size = vsum['unknown_size']
            total = active + deleted + unknown
            total_size = active_size+deleted_size+unknown_size
            unchanged.append(vk)
            n_unchanged = n_unchanged + 1
        #Do update the file information for volumes that have had there
        # last access time updated.
        else:
            if fd_output != 1:
                fd_output = os.open(os.path.join(output_dir, vv['external_label']),
                                    os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
                                    0666)
            print_header(vv, fd_output)

            # get all files of the volume

            q = "select f.bfid, v.label, f.size, f.location_cookie, f.deleted, f.pnfs_path \
                 from file f, volume v\
                 where v.label = '%s' and \
                 f.volume = v.id;"%(vk)
            res = file_db.query_dictresult(q)
            for f in res:  #bfids:
                os.write(fd_output, "%10s %15s %15s %22s %7s %s\n" % \
                    (f.get('label', "unknown"),
                     f.get('bfid', "unknown"),
                     f.get('size', "unknown"),
                     f.get('location_cookie', "unknown"),
                     f.get('deleted', "unknown"),
                     f.get('pnfs_path', "unknown")))
                n_files = n_files + 1

            # subtract from active counts number of active package files
            # that have constituent files marked deleted
            active  =  vv['active_files'] - volumes.get(vv['external_label'],0)
            deleted =  vv['deleted_files']
            unknown =  vv['unknown_files']
            active_size = vv['active_bytes']
            deleted_size = vv['deleted_bytes']
            unknown_size = vv['unknown_bytes']
            total = 0
            for k in ('active_files','deleted_files','unknown_files'):
                total += vv[k]
            total_size=0L
            for k in ('active_bytes','deleted_bytes','unknown_bytes'):
                total_size += vv[k]

            vsum = {
                'last' : vv['modification_time'],
                'active' : vv['active_files'],
                'deleted' : vv['deleted_files'],
                'unknown' : vv['unknown_files'],
                'total' : total,
                'active_size' : vv['active_bytes'],
                'deleted_size' : vv['deleted_bytes'],
                'unknown_size' : vv['unknown_bytes'],
                'total_size' : total_size,
                }
            vol_sum[vk] = vsum
            print_footer(vv, vsum, fd_output)
            #If the file is real, close it.
            if fd_output != 1:
                os.close(fd_output)
            #Update the number of volumes changed counter.
            n_changed = n_changed + 1

        ###
        ### Gather information about the volume's metadata.
        ###

        # is this a migrated volume?
        if (vv['system_inhibit'][1] == 'migrated' \
            or vv['system_inhibit'][1] == 'cloned') \
            and active == 0 \
            and vv['media_type'] != "null" \
            and vv['library'].find("shelf") == -1:
            mv_file.write("%s\t%s\t%d\t%s\t%s\t%s\n" % (
                vv['external_label'], vv['system_inhibit'][1],
                active, vv['media_type'], vv['library'], vv['volume_family']))
            n_migrated = n_migrated + 1
            migrated_vol = 1
        else:
            migrated_vol = 0

        # is this a duplication volume?
        if vv['system_inhibit'][1] == 'duplicated' \
               and active == 0 \
               and vv['media_type'] != "null" \
               and vv['library'].find("shelf") == -1:
            dv_file.write("%s\t%s\t%d\t%s\t%s\t%s\n" % (
                vv['external_label'], vv['system_inhibit'][1],
                active, vv['media_type'], vv['library'], vv['volume_family']))
            n_duplicated = n_duplicated + 1
            duplicated_vol = 1
        else:
            duplicated_vol = 0

        # can it be recycled?
        recyclable_vol = 0
        if vv['system_inhibit'][1] == 'full' \
            and active == 0 \
            and vv['media_type'] != "null" \
            and vv['library'].find("shelf") == -1:
            rc_file.write("%s\t%8s %6d %8s %10s\t%s\n" % (
                vv['external_label'], vv['system_inhibit'][1],
                vv['sum_mounts'], vv['media_type'], vv['library'],
                vv['volume_family']))
            n_recyclable = n_recyclable + 1
            recyclable_vol = 1

        # can it be recycled?
        if vv['system_inhibit'][1] == 'readonly' \
               and active == 0 \
               and vv['media_type'] != "null" \
               and vv['library'].find("shelf") == -1:
            rc_file2.append("%s\t%8s %6d %8s %10s\t%s\n" % (
                vv['external_label'], vv['system_inhibit'][1],
                vv['sum_mounts'], vv['media_type'], vv['library'],
                vv['volume_family']))
            n_recyclable2 = n_recyclable2 + 1
            recyclable_vol = 1

        # can it be recycled?
        if vv['volume_family'].find("DELETED_FILES") != -1 \
               and vv['volume_family'].find("-MIGRATION") == -1 \
               and vv['volume_family'].find("_copy_") == -1 \
               and active == 0 \
               and vv['media_type'] != "null" \
               and vv['library'].find("shelf") == -1:
            rc_contents_list3.append("%s\t%8s %6d %8s %10s\t%s\n" % (
                vv['external_label'], vv['system_inhibit'][1],
                vv['sum_mounts'], vv['media_type'], vv['library'],
                vv['volume_family']))
            number_of_recyclable3 = number_of_recyclable3 + 1
            recyclable_vol = 1

        # is this a deleted volume?
        if vv['system_inhibit'][0] == "DELETED":
            deleted_vol = 1
        else:
            deleted_vol = 0

        # check quota stuff
        storage_group = string.split(vv['volume_family'], '.')[0]
        library = vv['library']
        try:
            quota = quotas['libraries'][library][storage_group]
        except:
            quota = 'N/A'

        # is the volume written to or blank?  If blank, does the
        # write_protect value allow for writes?
	wp_n = 0
        if not total and vv['volume_family'][-10:] == '.none.none':
            written_vol = 0
            blank_vol = 1
            if vv['write_protected'] == 'n':
                wp_n = 1
        else:
            written_vol = 1
            blank_vol = 0

        #Increment the (library, storage_group) combination of counts.
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
                v_info[10] + unknown,
                v_info[11] + recyclable_vol,
                v_info[12] + migrated_vol,
                v_info[13] + wp_n,
                v_info[14] + duplicated_vol,
                )
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
                unknown,
                recyclable_vol,
                migrated_vol,
                wp_n,
                duplicated_vol,
                )

        # declaration errors
        if vk[:3] != 'CLN':
            actual_size = total_size+vv['remaining_bytes']
            if total_size <= 0:
                remark = 'never written'
            else:
                remark = ''

            #At FNAL the AML/2 didn't initially know what LTO tapes were.
            # So, those LTO tapes were entered as 3480.
            if vv['media_type'] == '3480' \
                   and vv['capacity_bytes'] == enstore_constants.CAP_LTO1:
                media_type = 'LTO1'
            elif vv['media_type'] == '3480' \
                   and vv['capacity_bytes'] == enstore_constants.CAP_LTO2:
                media_type = 'LTO2'
            else:
                media_type = vv['media_type']

            #Get the correct capacity based on the corrected media_type.
            #The raw value is in Gigabytes, so multiple by the number
            # of bytes in a Gigabyte.
            capacity = getattr(enstore_constants,
                               "CAP_" + vv['media_type'], None)
            try:
                #This is 1GB in base 2 (1024*1024*1024), or should it
                # be base 10 (10 ^ 9).  Going with base 2 for now.
                capacity = capacity * 1073741824
            except:
                #We get here if capacity is None.
                pass

            #lmt = Library Media Type
            lmt = library_media_types.get(vv['library'], None)

            de_values = (de_count, vk, actual_size, vv['capacity_bytes'],
                         vv['library'], media_type, remark)

            #Look for declaration errors.  The come in two types, where
            # the capacity was stated incorrectly and where the media type
            # of the library doesn't match the majority of the media types
            # for tapes beloning in that library.
            if capacity and actual_size \
               and (actual_size < (1 - CAPACITY_TOLERANCE) * capacity \
                    or actual_size > (1 + CAPACITY_TOLERANCE) * capacity):
                de_count = de_count + 1
                de_file.write(de_format % de_values)
            elif lmt and lmt != vv['media_type']:
                de_count = de_count + 1
                de_file.write(de_format % de_values)

        ###
        ### statistics stuff
        ###

        #last access time
        volume_sums[vk] = (active_size+deleted_size, deleted_size,
                           active_size)
        la_file.write("%f, %s %s\n" % (vv['modification_time'],
                time.ctime(vv['modification_time']), vv['external_label']))
        key = vv['external_label']
        la_values = (key,) + \
                format_storage_size(volume_sums[key][0]) + \
                format_storage_size(volume_sums[key][1]) + \
                format_storage_size(volume_sums[key][2]) + \
                format_storage_size(vv['capacity_bytes']) + \
                format_storage_size(vv['remaining_bytes']) + \
                format_storage_size(vv['capacity_bytes'] -
                                    vv['remaining_bytes']) + \
                (active, deleted, unknown, vv['volume_family'])
        la_format = \
           "%10s %7.2f%-2s %7.2f%-2s %9.2f%-2s %7.2f%-2s %7.2f%-2s %7.2f%-2s %8d %8d %8d %-s\n"
        vs_file.write(la_format % la_values)

        formated_size = format_storage_size(vv['remaining_bytes'])

        # too many mounts -- need more work
        if vv.has_key('sum_mounts'):
            mounts = vv['sum_mounts']
        else:
            mounts = -1
        mnts = "%6d"%(mounts) #This may be overridden to html tag format.
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

        #Determine if the tape is write protected or not.
        if vv['write_protected'] == 'y':
            wp = 'ON'
        elif vv['write_protected'] == 'n':
            wp = 'OFF'
        else:
            wp = '---'


        if vv['system_inhibit'][1] in wpa_states and \
               vv['media_type'] not in wpa_excluded_media_types and \
               not vv['library'] in wpa_excluded_libraries and \
               vv['library'].find("shelf") == -1:
            #vv['media_type'] in wpa_media_types and \

            n_rf_vols[vv['library']] = n_rf_vols.get(vv['library'], 0) + 1
            #Only set these next two, if the default hasn't already been
            # assigned.
            n_not_rp_vols[vv['library']] = n_not_rp_vols.get(vv['library'], 0)
            n_rp_vols[vv['library']] = n_rp_vols.get(vv['library'], 0)

            if wp != 'ON':
                wpa_values = (vv['external_label'], vv['system_inhibit'][1],
                              vv['library'], vv['media_type'], wp)
                n_not_rp_vols[vv['library']] = \
                                       n_not_rp_vols.get(vv['library'], 0) + 1
            else:
                n_rp_vols[vv['library']] = n_rp_vols.get(vv['library'], 0) + 1



        #volumes defined
        vd_file.write("%-10s %8.2f%2s (%-14s %8s) (%-8s  %8s) %-12s %-3s %6s %-40s\n" % \
               (vv['external_label'],
                formated_size[0], formated_size[1],
                vv['system_inhibit'][0],
                vv['system_inhibit'][1],
                vv['user_inhibit'][0],
                vv['user_inhibit'][1],
                vv['library'],
		wp,
                mnts,
                vv['volume_family']))

        #Update the total volume count and the volume count for the library.
        n_vols = n_vols + 1
        n_vols_lib[vv['library']] = n_vols_lib.get(vv['library'], 0) + 1

        if skipped == True :
            print 'skipped', time.time()-tt0
        else:
            print 'done', time.time()-tt0


    ###
    ### Dump that summarizes all the volumes metadata.
    ###

    # dump vol_sum
    print "volume_summary_cache_file:", volume_summary_cache_file
    sum_f = open(volume_summary_cache_file, 'w')
    cPickle.dump(vol_sum, sum_f)
    sum_f.close()

    #Append any summary information to these files.
    print_last_access_footer(la_file)
    print_volume_size_footer(vs_file)
    print_volumes_defined_footer(vd_file)
    print_too_many_mounts_footer(tm_file)
    print_migrated_volumes_footer(mv_file, n_migrated)
    print_duplicated_volumes_footer(dv_file, n_duplicated)
    print_recyclable_volumes_footer(rc_file, n_recyclable, rc_file2,
                                    n_recyclable2, rc_contents_list3,
                                    number_of_recyclable3)

    #Avoiding resource leaks, closing files.
    la_file.close()
    vs_file.close()
    vd_file.close()
    tm_file.close()
    de_file.close()
    mv_file.close()
    dv_file.close()

    # log remaing blanks into accounting db
    print "logging remaining_blanks to accounting db ...",
    res = file_db.dbaccess.query_getresult("select * from remaining_blanks")
    for i in res:
        q = "insert into blanks values('%s', '%s', %d)"%(time2timestamp(t0), i[0], i[1])
        acs.db.query(q)
    print 'done'

    # log wpa info once a day to the accounting DB
    hour = time.localtime(t0)[3]
    if hour == 22 :
        q = "insert into write_protect_summary (date, total, should, not_yet, done) values('%s', %d, %d, %d, %d);"%(time2timestamp(t0), n_vols, sum(n_rf_vols.values()), sum(n_not_rp_vols.values()), sum(n_rp_vols.values()))
        res = acs.db.query(q)
        # log individual numbers according to library
        for i in n_rf_vols.keys():
            q = "insert into write_protect_summary_by_library (date, library, total, should, not_yet, done) values('%s', '%s', %d, %d, %d, %d);"%(time2timestamp(t0), i, n_vols_lib[i], n_rf_vols[i], n_not_rp_vols[i], n_rp_vols[i])
            res = acs.db.query(q)

    #Remember to close the DB connections, too.

    db.close()
    vol_db.close()
    file_db.close()
    acs.db.close()

    # make a html copy
    # Do we need both a text and non-HTML version of this file?
    os.system('cp '+volumes_defined_file+' '+volumes_defined_file+'.html')
    os.system('sed -e "s/<font color=#FF0000>//g; s/<\/font>//g; s/<blink>//g; s/<\/blink>//g" '+volumes_defined_file+'.html > '+volumes_defined_file)

    #Create files that hold statistical data.
    fmt, tl = print_volume_quotas_status(volumes_allocated, authorized_tapes,
                               volume_quotas_file, quotas)
    print_volume_quota_sums(volumes_allocated, authorized_tapes,
                            volume_quotas_file, volume_quotas_format_file, fmt, tl)
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

    # if any of the directory is not there, create it
    for i in [inventory_dir, inventory_cache_dir]:
        if not os.access(i, os.F_OK):
            os.makedirs(i)

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

