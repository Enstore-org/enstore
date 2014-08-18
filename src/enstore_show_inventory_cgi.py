#!/usr/bin/env python

import os
import sys
import string

import enstore_functions2
import enstore_functions3
import configuration_client
import info_client
import e_errors

inv_dir = "/enstore/tape_inventory"

# Obtain the correct values for ENSTORE_CONFIG_HOST and ENSTORE_CONFIG_PORT
# if they are not already available.
def get_environment():
    #This is a very messy way to determine the host and port of the
    # configuration server.  But at least it is in only one place now.
    if os.environ.get('ENSTORE_CONFIG_HOST', None) == None:
        cmd = ". /usr/local/etc/setups.sh; setup enstore; echo $ENSTORE_CONFIG_HOST; echo $ENSTORE_CONFIG_PORT"
        pfile = os.popen(cmd)
        config_host = pfile.readline().strip()
        if config_host:
            os.environ['ENSTORE_CONFIG_HOST'] = config_host
        else:
            print "Unable to determine ENSTORE_CONFIG_HOST."
            sys.exit(1)
        config_port = pfile.readline().strip()
        if config_port:
            os.environ['ENSTORE_CONFIG_PORT'] = config_port
        else:
            print "Unable to determine ENSTORE_CONFIG_PORT."
            sys.exit(1)
        pfile.close()

def setup(csc):

    #Determine the local inventory directory.
    inventory_config_dict = csc.get("inventory", 3, 3)
    raw_inv_dir = None
    if e_errors.is_ok(inventory_config_dict):
        raw_inv_dir = inventory_config_dict.get("inventory_rcp_dir", "")
    if not raw_inv_dir:
        print "Unable to determine inventory | inventory_rcp_dir from config file."
        sys.exit(1)
    #Remove the hostname that is at the beginning of this string.
    raw_inv_dir = string.join(raw_inv_dir.split(":")[1:], "")

    #Obtain the contents of the local inventory directory.
    special = []
    dir_contents = os.listdir(raw_inv_dir)
    if not dir_contents:
            sys.stderr.write("No tape_inventory directory contents found.\n")
            sys.exit(1)


    #Determine the correct name for the cluster.  This is useful when
    # you have more than one Enstore system.
    crons_config_dict = csc.get("crons", 3, 3)
    cluster = None
    if e_errors.is_ok(crons_config_dict):
        cluster = crons_config_dict.get("enstore_name", "")

    #Loop over the contents of the directory looking for non-external_label
    # style names.
    dir_contents.sort()
    special = []
    for fname in dir_contents:
            if fname[0] == ".":
                    continue #skip hidden files.
            if not enstore_functions3.is_volume(fname):
                    #full_path = os.path.join(raw_inv_dir, fname)
                    special.append(fname)

    return (special, cluster)

def get_volumes(csc):
    #Get the information server client.
    ifc = info_client.infoClient(csc, rcv_timeout = 3, rcv_tries = 3)

    ###Build up the listing of all volumes currently defined to Enstore.
    catalog = {}
    #Only get here when the full inventory needs to be displayed.
    ticket = ifc.get_vol_list()
    if e_errors.is_ok(ticket):
        for i in ticket['volumes']:
            f = string.strip(i)
            if f[-8:] != '.deleted':
                    prefix = f[:3]
                    if catalog.has_key(prefix):
                            catalog[prefix].append(f)
                    else:
                            catalog[prefix] = [f]

    return catalog

#If catalog is an empty directory, then display just the summary.  Otherwise,
# catalog should be a dictionary sorted into groups based on the first
# few letters of the volume.  enstore_show_inv_summary_cgi.py passes an
# empty dictionary here to generate the summary page.
#
#Special is a list of fnames in the tape_inventory directory that are not
# volume names.
#
#Cluster is the name of the current Enstore system.
def print_html(catalog, special, cluster):

    # in the beginning ...

    print "Content-type: text/html"
    print

    # taking care of the header

    if catalog:
        use_title = "Enstore Tape Inventory"
    else: #Just summary
        use_title = "Enstore Tape Inventory Summary"
    if cluster:
        use_title = "%s on %s" % (use_title, cluster)

    print '<html>'
    print '<head>'
    print '<title>' + use_title + '</title>'
    print '</head>'
    print '<body bgcolor="#ffffd0">'

    print '<font size=7 color="#ff0000">' + use_title + '</font>'
    print '<hr>'

    # output the index

    if catalog:  #Only do for full inventory.
        print '<h2><font color="#aa0000">Index</font></h2>'

        keys = catalog.keys()
        keys.sort()
        for i in keys:
            print '<a href=#' + i + '>' + i + '</a>&nbsp;&nbsp;'

    # handle special files

    if catalog: #Only output the special file header for the full inventory.
        print '<hr>'
        print '<p>'
        print '<h2><font color="#aa0000">Special Files</font></h2>'

    print '<p>'
    for fname in special:
        if fname == 'COMPLETE_FILE_LISTING':
            print '<a href="enstore_file_listing_cgi.py">', string.split(fname, '.')[0], '</a>&nbsp;&nbsp;'
        elif fname.startswith(("COMPLETE_FILE_LISTING","CHIMERA_DUMP","RECENT_FILES_ON_TAPE")):
            #We hide these behind the cgi that COMPLETE_FILE_LISTING points to.
            continue
        else:
            print '<a href="' + os.path.join(inv_dir, fname) + '">', string.split(fname, '.')[0], '</a>&nbsp;&nbsp;'

        if not catalog:
            #Only do for summary.
            print '<br>'

    print '<a href="enstore_recent_files_on_tape_cgi.py">', "RECENT_FILES_ON_TAPE", '</a>&nbsp;&nbsp;'
    ### The raw directory listing used to work.  But now that the security
    ### baseline STRONGLY recommends that this be disabled and we have
    ### disabled it; it does not make sense to include a link to something
    ### no one has permissions to see.
    print '<p><a href="' + inv_dir + '">Raw Directory Listing</a>'

    # output the main volume listing

    if catalog:
        for i in keys:
            print '<hr>'
            print '<p>'
            print '<h2><a name="' + i + '"><font color="#aa0000">' + i + '</font></a></h2>'
            for j in catalog[i]:
                print '<a href=/cgi-bin/show_volume_cgi.py?volume=' + j +'>', j, '</a>&nbsp;&nbsp;'

    # the end
    print '</body>'
    print '</html>'



def main():
    # Obtain the correct values for ENSTORE_CONFIG_HOST and ENSTORE_CONFIG_PORT
    # if they are not already available.
    get_environment()

    #Get the configuration server client.
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = configuration_client.ConfigurationClient((config_host, config_port))

    #Get information.
    (special, cluster) = setup(csc)

    #Get the list of volumes.
    catalog = get_volumes(csc)

    #
    #Print the html output.
    #
    print_html(catalog, special, cluster)


if __name__ == '__main__':

    main()
