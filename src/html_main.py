#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# system imports
from __future__ import print_function
import sys
#import re
import os
#import grp
#import pwd

# enstore imports
#import configuration_client
import option
#import enstore_up_down
import e_errors
import Trace
#import enstore_functions2
import enstore_constants
#import enstore_files
#import enstore_plots

import enstore_saag
import enstore_saag_network
import enstore_system_html
import enstore_make_plot_page
import enstore_make_generated_page


class HtmlInterface(option.Interface):
    def __init__(self, args=sys.argv, user_mode=0):
        # for html options
        self.pages = None
        self.plots = None
        self.saag_network = None
        self.saag = None
        self.system_html = None

        # for saag options
        self.html_gen_host = None

        option.Interface.__init__(self, args=args, user_mode=user_mode)

    def valid_dictionaries(self):
        return (self.help_options, self.html_options, self.saag_options)

    #  define our specific parameters
    parameters = []

    html_options = {
        option.PAGES: {option.HELP_STRING:
                       "Make the generated pages web pages.",
                       option.VALUE_USAGE: option.IGNORED,
                       option.VALUE_TYPE: option.INTEGER,
                       option.USER_LEVEL: option.USER, },
        option.PLOTS: {option.HELP_STRING:
                       "Make the plot web pages.",
                       option.VALUE_USAGE: option.IGNORED,
                       option.VALUE_TYPE: option.INTEGER,
                       option.USER_LEVEL: option.USER, },
        option.SAAG_NETWORK: {option.HELP_STRING:
                              "Make the SAAG Network web page.",
                              option.VALUE_USAGE: option.IGNORED,
                              option.VALUE_TYPE: option.INTEGER,
                              option.USER_LEVEL: option.USER, },
        option.SAAG: {option.HELP_STRING:
                      "Make the Status-At-a-Glance web page.",
                      option.VALUE_USAGE: option.IGNORED,
                      option.VALUE_TYPE: option.INTEGER,
                      option.USER_LEVEL: option.USER, },
        option.SYSTEM_HTML: {option.HELP_STRING:
                             "Make the top Enstore system web page.",
                             option.VALUE_USAGE: option.IGNORED,
                             option.VALUE_TYPE: option.INTEGER,
                             option.USER_LEVEL: option.USER, },
    }

    saag_options = {
        option.HTML_GEN_HOST: {option.HELP_STRING:
                               "ip/hostname of the html server",
                               option.VALUE_TYPE: option.STRING,
                               option.VALUE_USAGE: option.REQUIRED,
                               option.VALUE_LABEL: "node_name",
                               option.USER_LEVEL: option.ADMIN,
                               },
    }


# The main function takes the interface class instance as parameter and
# returns an exit status.
def main(intf):
    if intf.saag_network:
        print("Making the SAAG Network web page.")
        enstore_saag_network.do_work(intf)
    if intf.saag:
        print("Making the SAAG web page.")
        enstore_saag.do_work(intf)
    if intf.system_html:
        print("Making the top Enstore system web page.")
        enstore_system_html.do_work(intf)
    if intf.plots:
        print("Making the plot web pages.")
        enstore_make_plot_page.do_work2(intf)
    if intf.pages:
        print("Making the generated web pages web pages.")
        enstore_make_generated_page.do_work(intf)


def do_work(intf):

    try:
        exit_status = main(intf)
    except (SystemExit, KeyboardInterrupt):
        exc, msg = sys.exc_info()[:2]
        Trace.log(
            e_errors.ERROR, "migrate aborted from: %s: %s" %
            (str(exc), str(msg)))
        exit_status = 1
    except BaseException:
        # Get the uncaught exception.
        exc, msg, tb = sys.exc_info()
        print("Uncaught exception:", exc, msg)
        # Send to the log server the traceback dump.  If unsuccessful,
        # print the traceback to standard error.
        Trace.handle_error(exc, msg, tb)
        del tb  # No cyclic references.
        exit_status = 1

    sys.exit(exit_status)


if __name__ == '__main__':
    intf_of_html = HtmlInterface(sys.argv, 0)  # zero means admin

    do_work(intf_of_html)
