#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# system imports
import os
import sys
import string

# enstore imports
import enstore_files
import enstore_constants
import generic_client
import option
import configuration_client


def find_web_files(search_directory):

    file_list = []
    
    for filename in os.listdir(search_directory):
        fullname = os.path.join(search_directory, filename)

        if fullname[0] == ".":
            continue   #skip hidden files
        #if not os.path.isfile(fullname):
        #    continue   #skip non-regular files
        
        file_list.append(fullname)

    return file_list

# full_subdir_path : Absolute path to the directory containing plots
#                    to have the a plot page created.
# url_gif_dir : URL path for finding the gifs for things like the
#               page background.
# plot_name : A string describing the contents of the plots in the
#             plot directory.
# links_l : Only the top page should specifiy this.  It is a list of
#           tuples containing the sub-plot-directories and the description
#           of the plots.
def make_page(full_subdir_path, url_gif_dir, page_name, links_l = None):

    #Append the pages filename that will be created in this directory.
    pages_basename = enstore_files.generated_web_page_html_file_name()
    pages_file = os.path.join(full_subdir_path, pages_basename)

    #For all the link dirs, we need to append the plots filename that
    # will be created in this directory.
    use_links_l = []
    if links_l:
        for link_dir,link_name in links_l:
            use_links_l.append((os.path.join(link_dir, pages_basename),
                               link_name))

    #Obtain the list of files in the directory so that links can be made
    # to point to them.
    dir_list = find_web_files(full_subdir_path)
    #We need to remove the output file itself from the list.
    try:
        dir_list.remove(pages_file)
    except ValueError:
        pass
    #Loop over the directory links and remove them from the list.  We are
    # looking for files, since the directories have already been identified.
    # Remember, directories get links to the top html file in those
    # directories.
    if links_l:
        for link_dir, link_name in links_l:
            #Search the list for a match.
            for i in range(len(dir_list)):
                #If the path ends witht the URL link directory, then it is
                # a directory we should ignore.
                if dir_list[i].endswith("/" + link_dir):
                    try:
                        del dir_list[i]
                    except ValueError:
                        pass

    #Override use of system_tag to contain the name of the plot page.
    system_tag = page_name
    html_of_pages = enstore_files.HTMLGeneratedFile(pages_file, system_tag,
                                                    url_gif_dir,
                                                    url_gif_dir = url_gif_dir)
    html_of_pages.open()
    mount_label = "" #???
    html_of_pages.write(dir_list, mount_label, use_links_l)
    html_of_pages.close()
    html_of_pages.install()


def do_work(intf):
    # Get the configuration server.
    csc = configuration_client.ConfigurationClient((intf.config_host,
                                                    intf.config_port))
    # Get directory information we are going to need.
    crons_dict = csc.get('crons', {})
    html_dir = crons_dict.get('html_dir', None)
    url_dir = crons_dict.get('url_dir', "")

    if not html_dir:
        sys.stderr.write("Unable to determine html_dir.\n")
        return True
    if not os.path.isdir(html_dir):
        sys.stderr.write("Directory %s does not exist.\n" % (html_dir,))
        return True

    generated_pages_subdir = os.path.join(html_dir, "")
    if not os.path.isdir(generated_pages_subdir):
        try:
            os.makedirs(generated_pages_subdir)
        except (OSError, IOError), msg:
            sys.stderr.write("Failed to create directory %s: %s\n" %
                             (generated_pages_subdir, str(msg)))
            return True

    #Is there a better place for this list?
    subdir_description_list = [
        (enstore_constants.WEEKLY_SUMMARY_SUBDIR,
         "Weekly statistics on the number of tapes, bytes and cleaning tapes used."),
        (enstore_constants.MISC_HTML_SUBDIR,
         "Miscellaneous Enstore web pages."),
        ]

    use_subdir_list = []
    #Loop over all the subdirs making pages.
    for subdir, page_name in subdir_description_list:
        full_subdir_path = os.path.join(generated_pages_subdir, subdir)
        #The top directory needs some additional attention.
        if full_subdir_path == generated_pages_subdir:
            continue
        #Skip if the directory does not exist.
        if not os.path.isdir(full_subdir_path):
            continue

        make_page(full_subdir_path, url_dir, page_name)

        #We need to remove the top directory in the path since,
        # generated_page_subdir already has it.  If we don't remove it here
        # then we have enstore_constants.WEB_SUBDIR repeated twice in the
        # URL path.
        use_subdir_for_link = string.join(os.path.split(subdir)[1:], "/")
        use_subdir_list.append((use_subdir_for_link, page_name))

    #Create the top page.
    generated_pages_subdir = os.path.join(html_dir,
                                          enstore_constants.WEB_SUBDIR)
    #If necessary, create the directory the file will go to.
    if not os.path.isdir(generated_pages_subdir):
        try:
            os.makedirs(generated_pages_subdir)
        except (OSError, IOError), msg:
            sys.stderr.write("Failed to create directory %s: %s\n" %
                             (generated_pages_subdir, str(msg)))
            return True
    make_page(generated_pages_subdir, url_dir, "Enstore Generated Web Pages",
              use_subdir_list)

    return False


class GeneratedPageInterface(generic_client.GenericClientInterface):

    def __init__(self, args=sys.argv, user_mode=1):
	generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)
    plot_options = {}

    def valid_dictionaries(self):
	return (self.help_options, self.plot_options)


if __name__ == "__main__":   # pragma: no cover

    intf = GeneratedPageInterface(user_mode=0)

    sys.exit(do_work(intf))
