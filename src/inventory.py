#!/usr/bin/env python

#system imports
import sys
import time
import string
import os
import tempfile

#user imports
import db


class match_list:
    def __call__(self, list_element):
        return list_element['external_label'] == self.compare_value
    def __init__(self, thresh):
        self.compare_value = thresh

#Print the command line syntax to the terminal.
def inventory_usage(message):
    print "\n" + message + "\n"
    print "Usage: inventory.py <volume file> <data file>\n"


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
        t0 = time.time()
        k,v = d.cursor('first')

        while k:
            count=count+1
            v=d[k]
            list.append(v)
            k,v=d.cursor('next')
            if count % 1000 == 0:
                delta = time.time()-t0
                print "Through line %d rate is %.1f keys/S in " \
                      "%d seconds total." % \
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

    out_string = "GBytes free:\t  %d\n" % \
                 int(volume['remaining_bytes'] / 1048576)
    os.write(fd, out_string)

    out_string = "GBytes written:\t  %d\n" % \
                 int((volume['capacity_bytes'] - volume['remaining_bytes']) \
                     / 1048576)
    os.write(fd, out_string)

    out_string = "Inhibits:\t  %s+%s\n\n" % \
          (volume['system_inhibit'][0], volume['user_inhibit'][0])
    os.write(fd, out_string)

    out_string = "%10s %15s %10s %22s %7s %s\n" % \
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
        return ['']
    
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

#Print the sums of the file sizes to the file USAGE.
def print_volume_size_stats(volume_sums, output_dir):
    usage_file = open(output_dir + "USAGE", "w")
    usage_file.write("%10s %15s\n" % ("Label", "Size"))
    sorted_keys = volume_sums.keys()
    sorted_keys.sort()
    for key in sorted_keys:
        usage_file.write("%10s %15s\n" % (key, str(volume_sums[key])))

#Print the last access info to the output file LAST_ACCESS.
def print_last_access_status(volume_list, output_dir):
    volume_list.sort(la_sort)
    la_file = open(output_dir + "LAST_ACCESS", "w")
    for volume in volume_list:
        la_file.write("%f, %s %s\n"
                      % (volume['last_access'],
                         time.asctime(time.localtime(volume['last_access'])),
                         volume['external_label']))

def print_volumes_defind_status(volume_list, output_dir):
    volume_list.sort(el_sort)
    vd_file = open(output_dir + "VOLUMES_DEFINED", "w")
    for volume in volume_list:
        vd_file.write("%10s %s\n"
                      % (volume['external_label'],
                         volume['remaining_bytes']))

        
##############################################################################
##############################################################################

def create_fd_list(volume_list, output_dir):
    #Create the file descriptor dictionary.
    fd_list = {}
    
    #Loop through each entry in the list of volumes.  Pull out all files
    # from the file data list on each particular volume.  Output the data
    # in the appropriate format.
    for volume in volume_list:
        if output_dir == None:
            fd = 1
            print "setting file descriptor to 1."
        else:
            file_string = output_dir + volume['external_label']
            fd = os.open(file_string,
                         os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0666)
            if fd == -1:
                print "Error opening file " + file_string + "."
                sys.exit(1)
        
        #Place the fd in the file descriptor dictionary.
        fd_list[volume['external_label']] = fd

    return fd_list

def process_out_string(short_list, fd_list):
    for vol in short_list:
        try:
            out_string = "%10s %15s %10s %22s %7s %s\n" % \
                         (vol['external_label'],
                          vol['bfid'],
                          vol['size'],
                          vol['location_cookie'],
                          vol['deleted'],
                          vol['pnfs_name0'])
            
        #there is a possibility that the last two options (deleted and
        # pnfs_name0) are not present.  Print 'unknown' in their place.
        except KeyError:
            out_string = "%10s %15s %10s %22s %7s %s\n" % \
                         (vol['external_label'],
                          vol['bfid'],
                          vol['size'],
                          vol['location_cookie'],
                          "unknown",
                          "unknown")
            
        #It doesn't matter if the value in out_string was set with or
        # without an exception being through.  Just stream it out.
        os.write(fd_list[vol['external_label']], out_string)


def parse_time(seconds):
    hour = int(seconds) / 60 / 60
    min = int(seconds) / 60
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


def verify_volume_sizes(volume_data, volume, volume_sizes):
    sum_size = 0
    #Keep some statistical analysis going:
    # Count the sum of all the file sizes on a given volume.
    try:
        for line in volume_data:
            some_string = string.split(line)[2]
            if some_string[-1] == "L":
                some_string = some_string[0:-1] #remove appending 'L'
            sum_size = sum_size + long(some_string)
    except IndexError:
        sum_size = 0 #This exception occurs on empty files. So, this is zero.

    volume_sizes[volume['external_label']] = sum_size

##############################################################################
##############################################################################

#Proccess the inventory of the files specified.  This is the main source
# function where all of the magic starts.
#Takes the full filepath name to the volume file in the first parameter.
#Takes the full filepath name to the data file in the second parameter.
#Takes the full path to the ouput directory in the third parameter.

def inventory(volume_file, data_file, output_dir):
    t0 = time.time() #grab the start time
    count_1000s = 0 #keep track of the number of files processed.

    volume_list = read_db(os.path.split(volume_file))
    if volume_list == 1:
        print "Database " + volume_file + " read in unsuccessfully."

    #A cool thing this class is.  It is a class, but it acts like a function
    # later on.  This is how the filter function allows me to pass in
    # different filter values.
    compare = match_list("")


    os.system("rm -rf /tmp/label_files/")
#    os.remove("/tmp/label_files/*")
#    os.rmdir("/tmp/label_files/")

    os.mkdir("/tmp/label_files/", 0777)
    
    #The variable fd_tmp is really a dictionary, but holds a list of file
    # descriptors to temporary output file. 
    fd_tmp = create_fd_list(volume_list, "/tmp/label_files/")

    data_list = [1]   #Give it an element to make it have a non-zero length.
    volume_sums = {}  #The summation of all of the file sizes on a volume.
    db = None         #The database needed by read_long_db()

    #While there is still data to process.
    while len(data_list):

        #Get the data list in groups of 1000 records.  For the first call,
        # pass None into second parameter, this will tell the function to
        # create the database instance.
        long_read_return = read_long_db(os.path.split(data_file), db, 1000)

        #For readability rename the two parts of the return value.
        data_list = long_read_return[0]
        db = long_read_return[1]

        for volume in volume_list:
            #This is where the magic of that callable class happends.  I set
            # the compare_value variable and it is automatically referenced
            # when the class is executed like a function in the filter() call.
            compare.compare_value = volume['external_label']
            short_list = filter(compare, data_list)

            #With the short list of files located on the current volume
            # print them out to the corresponding temporary file.
            process_out_string(short_list, fd_tmp)

        #Since, the while loop takes a while to process all of the data,
        # generate some running performace statistics.
        delta = time.time()-t0
        count_1000s = count_1000s + len(data_list)
        print "Through line %d rate is %.1f keys/S in %s." % \
              (count_1000s, count_1000s/delta, parse_time(delta))

#        break #usefull for debugging

    #The variable fd_ouput is really a dictionary, but holds a list of file
    # descriptors to corresponding output files. 
    fd_output = create_fd_list(volume_list, output_dir)

    #print information to a real file.
    for volume in volume_list:
        #Print the header information.
        print_header(volume, fd_output[volume['external_label']])

        #Prints the data to the appropriot output file.  Also, returns a
        # list of the lines in the tmp file.
        file_data = print_data(volume, fd_tmp[volume['external_label']],
                               fd_output[volume['external_label']])

        #Print the footer information.
        print_footer(volume, fd_output[volume['external_label']])

        #Verifies the amount of data stored on the volumes.  Each call to
        # this function adds an entry into volume_sums.  The data generated
        # will be outputed by the print_volume_size_stats() funciton.
        verify_volume_sizes(file_data, volume, volume_sums)
        
        #Print current time information to the screen.
        print "Label", volume['external_label'], "processed.",
        print "(%s)" % (parse_time(time.time() - t0))

    #Create files that hold statistical data.
    print_last_access_status(volume_list, output_dir)
    print_volume_size_stats(volume_sums, output_dir)
    print_volumes_defind_status(volume_list, output_dir)


    try:
        os.rmdir("/tmp/label_files/")
    except OSError:
        pass #We want the dir to go away, if it's already gone don't worry.

    #Print stats regarding the data generated.
    delta_t = time.time() - t0
    print "%d files on %d volumes processed in %s." % \
          (count_1000s, len(volume_list), parse_time(delta_t))


                
if __name__ == "__main__":
    if len(sys.argv) < 3:
        inventory_usage("To few arguments.")
    elif len(sys.argv) == 3:
        #volume file, data file, None == stdout
        inventory(sys.argv[1], sys.argv[2], None)
    elif len(sys.argv) == 4:
        #The user shouldn't have to enter in a "/" at the end of the directory.
        # If it is not there add it.
        if sys.argv[3][-1] != "/":
            sys.argv[3] = sys.argv[3] + "/"
        #volume file, data file, ouput directory
        inventory(sys.argv[1], sys.argv[2], sys.argv[3])
    else:
        inventory_usage("To many arguments.")
