#!/usr/bin/env python

############################################################################
"""
Example option group dictionaries:

To set a simple value: option.py --opt
   example_options = {
       'opt':{HELP_STRING:"some string text"}
       }
The preceding will do the same thing as the full dictionary example:
   example_options = {
       'opt':{HELP_STRING:"some string text"}
              DEFAULT_NAME:'opt',
              DEFAULT_VALUE:1,
              DEFAULT_TYPE:option.STRING,
              VALUE_USAGE:option.IGNORED,
              USER_LEVEL:option.USER,
              FORCE_SET_DEFAULT:0,
              EXTRA_VALUES:[],
        }
    }

To set two values, one saying the option was specifed as a boolean and another
to hold the actuall value (a filename for example): option.py --opt <filename>
    example_options = {
       'opt':{HELP_STRING:"some string text"}
              DEFAULT_NAME:'opt',
              DEFAULT_VALUE:option.DEFAULT,
              DEFAULT_TYPE:option.INTEGER,
              VALUE_NAME:'filename'
              VALUE_TYPE:option.STRING,
              VALUE_USAGE:option.REQUIRED,
              VALUE_LABEL:"filename",
              USER_LEVEL:option.USER,
              FORCE_SET_DEFAULT:option.FORCE,
              EXTRA_VALUES:[],
        }
    }

To accept multiple values: option.py --opt <filename> [filename2]
    example_options = {
       'opt':{HELP_STRING:"some string text"}
              DEFAULT_NAME:'opt',
              DEFAULT_VALUE:1,
              DEFAULT_TYPE':option.INTEGER,
              VALUE_NAME:'filename'
              VALUE_TYPE:option.STRING,
              VALUE_USAGE:option.REQUIRED,
              VALUE_LABEL:"filename",
              USER_LEVEL:option.USER,
              FORCE_SET_DEFAULT:1,
              EXTRA_VALUES:[{DEFAULT_NAME:"filename2",
                             DEFAULT_VALUE:"",
                             DEFAULT_TYPE:option.STRING,
                             VALUE_NAME:"filename2",
                             VALUE_TYPE:option.STRING,
                             VALUE_USAGE:option.OPTIONAL,
                             VALUE_LABEL:"filename2",
                               }]
        }
    }

"""
############################################################################

import os
import sys
import string
import pprint
import getopt
import fcntl
import TERMIOS

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = '7500'

#default value
DEFAULT = 1

#default help string
BLANK = ""

#existance of command value
REQUIRED = "required"
OPTIONAL = "optional"
IGNORED  = "ignored"

#command level
USER = "user"
ADMIN = "admin"

#variable type
INTEGER = "integer"
STRING = "string"
FLOAT = "float"
RANGE = "range"

#default action
FORCE = 1
NORMAL = 0

#strings to use in the dictionaries.
HELP_STRING = "help string"
DEFAULT_NAME = "default name"
DEFAULT_VALUE = "default value"
DEFAULT_TYPE = "default type"
VALUE_NAME = "value name"
VALUE_TYPE = "value type"
VALUE_USAGE = "value usage"
VALUE_LABEL = "value label"
USAGE_LEVEL = "user level"
SHORT_OPTION = "short option"
FORCE_SET_DEFAULT = "force set default"
USER_LEVEL = "user level"
EXTRA_VALUES = "extra values"

############################################################################

#Note: This list is in alphabetical order, please keep it that way.
ADD = "add"                                  #volume
ALIVE = "alive"
ALL = "all"                                  #volume
BACKUP = "backup"                            #volume, file
BFID = "bfid"                                #pnfs, file
BFIDS = "bfids"                              #file
CAPTION_TITLE = "caption-title"              #enstore_make_log_plot
CHECK = "check"                              #volume
CLEAN_DRIVE = "clean_drive"                  #mover
CLEAR = "clear"                              #volume
CONFIG_FILE = "config-file"                  #configuration(c&s)
CONST = "const"                              #pnfs
COUNTERS = "counters"                        #pnfs
COUNTERSN = "countersN"                      #pnfs
CP = "cp"                                    #pnfs
CURSOR = "cursor"                            #pnfs
DATABASE = "database"                        #pnfs
DATABASEN = "databaseN"                      #pnfs
DECR_FILE_COUNT = "decr-file-count"          #volume
DELETE = "delete"                            #volume
DELETED = "deleted"                          #file
DELETE_WORK = "delete-work"                  #library
DESCRIPTION = "description"                  #enstore_make_log_plot
DESTROY = "destroy"                          #volume
DISMOUNT = "dismount"                        #media
DO_ALARM = "do-alarm"
DONT_ALARM = "dont-alarm"
DO_LOG = "do-log"
DONT_LOG = "dont-log"
DO_PRINT = "do-print"
DONT_PRINT = "dont-print"
DOWN = "down"                                #pnfs, inqusitor, mover
DUMP = "dump"                                #pnfs, alarm, inquisitor, mover
DUPLICATE = "duplicate"                      #pnfs
ECHO = "echo"                                #pnfs
ENCP = "encp"                                #plotter
ENSTORE_STATE = "enstore-state"              #pnfs
ERASE = "erase"                              #volume
EXPORT = "export"                            #volume
FILE_FAMILY = "file-family"                  #pnfs
FILE_FAMILY_WIDTH = "file-family-width"      #pnfs
FILE_FAMILY_WRAPPER = "file-family-wrapper"  #pnfs
FILES = "files"                              #pnfs
FILESIZE = "filesize"                        #pnfs
FORCE = "force"                              #volume
FORGET_ALL_IGNORED_STORAGE_GROUPS = "forget-all-ignored-storage-groups"   #volume
FORGET_IGNORED_STORAGE_GROUP = "forget-ignored-storage-group"   #volume
GET_CRCS = "get-crcs"                        #file
GET_LAST_LOGFILE_NAME = "get-last-logfile-name"  #log
GET_LOGFILE_NAME = "get-logfile-name"        #log
GET_LOGFILES = "get-logfiles"                #log
GET_MAX_ENCP_LINES = "get-max-encp-lines"    #inquisitor
GET_QUEUE = "get-queue"                      #library
GET_REFRESH = "get-refresh"                  #inquisitor
GET_SUSPECT_VOLS = "get-suspect-vols"        #library
GET_UPDATE_INTERVAL = "get-update-interval"  #inquisitor
GET_WORK = "get-work"                        #library, media
GET_WORK_SORTED = "get-work-sorted"          #library
HELP = "help"
HOST = "host"                                #monitor
HTML = "html"                                #up_down
HTML_DIR = "html-dir"                        #monitor(server)
HTML_FILE = "html-file"                      #inquisitor(server)
HTML_GEN_HOST = "html-gen-host"              #monitor, system
ID = "id"                                    #pnfs
IGNORE_STORAGE_GROUP = "ignore-storage-group"   #volume
IMPORT = "import"                            #volume
IO = "io"                                    #pnfs
LAYER = "layer"                              #pnfs
LIBRARY = "library"                          #pnfs
LIST = "list"                                #volume, file
LOAD = "load"                                #configuration
LOG = "log"                                  #medaia(s)
KEEP = "keep"                                #plotter
KEEP_DIR = "keep-dir"                        #plotter
LOGFILE_DIR = "logfile-dir"                  #plotter
LS = "ls"                                    #pnfs
LS_ACTIVE = "ls-active"                      #volume, file
MAX_ENCP_LINES = "max-encp-lines"            #inquisitor(c&s)
MAX_WORK = "max-work"                        #media(c&s)
MESSAGE = "message"                          #log
MODIFY = "modify"                            #volume
MOUNT = "mount"                              #media, plotter
NAMEOF = "nameof"                            #pnfs
NEW_LIBRARY = "new-library"                  #volume
NO_ACCESS = "no-access"                      #volume
NO_MAIL = "no-mail"                          #up_down
NOTIFY = "notify"                            #notify
NOOUTAGE = "nooutage"                        #inquisitor
NOOVERRIDE = "nooverride"                    #inquisitor
OFFLINE = "offline"                          #mover
ONLINE = "online"                            #mover
OUTAGE = "outage"                            #inquisitor
OUTPUT_DIR = "output-dir"                    #plotter
OVERRIDE = "override"                        #inquisitor
RECURSIVE = "recursive"                      #file
REFRESH = "refresh"                          #inquisitor(c&s)
PARENT = "parent"                            #pnfs
PATH = "path"                                #pnfs
PNFS_STATE = "pnfs-state"                    #pnfs
POSITION = "position"                        #pnfs
PREFIX = "prefix"                            #enstore_make_log_plot
PRIORITY = "priority"                        #library
RAISE = "raise"                              #alarm
READ_ONLY = "read-only"                      #volume
RECYCLE = "recycle"                          #volume
RESET_LIB = "reset-lib"                      #volume
RESOLVE = "resolve"                          #alarm
RESTORE = "restore"                          #volume, file
RETRIES ="retries"
RM = "rm"                                    #pnfs
RM_ACTIVE_VOL = "rm-active-vol"              #library
RM_SUSPECT_VOL = "rm-suspect-vol"            #library
ROOT_ERROR = "root-error"                    #alarm
SAAG_STATUS = "saagstatus"                   #inquisitor
SENDTO = "sendto"                            #mover
SET_CRCS = "set-crcs"                        #file
SEVERITY = "severity"                        #alarm
SG = "sg"                                    #plotter
SHOW = "show"                                #configuration, inquisitor, media
SHOW_IGNORED_STORAGE_GROUPS = "show-ignored-storage-groups"   #volume
SHOWID = "showid"                            #pnfs
SIZE = "size"                                #pnfs
START_DRAINING = "start-draining"            #library
START_TIME = "start-time"                    #plotter
STATUS = "status"                            #mover, library
STOP_DRAINING = "stop-draining"              #library
STOP_TIME = "stop-time"                    #plotter
STORAGE_GROUP = "storage-group"              #pnfs
SUBSCRIBE = "subscribe"                      #inquisitor
SUMMARY = "summary"                          #monitor, configuration, up_down
TAG = "tag"                                  #pnfs
TAGECHO = "tagecho"                          #pnfs
TAGRM = "tagrm"                              #pnfs
TAGS = "tags"                                #pnfs
TIME = "time"                                #inquisitor
TIMEOUT = "timeout"
TITLE = "title"                              #enstore_make_log_plot
UP = "up"                                    #pnfs, inquisitor, mover
UPDATE = "update"                            #inquisitor
UPDATE_AND_EXIT = "update-and-exit"          #inquisitor
UPDATE_INTERVAL = "update-interval"          #inquisitor(c&s)
USAGE = "usage"
VOL = "vol"                                  #volume
VOLS = "vols"                                #volume, library
VOLUME = "volume"                            #pnfs
VOL1OK = "VOL1OK"                            #volume
WARM_RESTART = "warm-restart"                #mover
WEB_HOST = "web-host"                        #enstore_make_log_plot
XREF = "xref"                                #pnfs

#these are this files test options
OPT = "opt"                                  #option
TEST = "test"                                #option

#This list is the master list of options allowed.  This is in an attempt
# to keep the different spellings of options (ie. --host vs. --hostip vs --ip)
# in check.
valid_option_list = [
    ADD, ALIVE, ALL,
    BACKUP, BFID, BFIDS,
    CAPTION_TITLE, CHECK, CLEAN_DRIVE, CLEAR, CONFIG_FILE, CONST,
    COUNTERS, COUNTERSN, CP, CURSOR,
    DATABASE, DATABASEN,
    DECR_FILE_COUNT, DELETE, DELETED, DELETE_WORK, DESCRIPTION, DESTROY,
    DISMOUNT,
    DO_ALARM, DONT_ALARM, DO_LOG, DONT_LOG, DO_PRINT, DONT_PRINT, DOWN,
    DUMP, DUPLICATE,
    ECHO, ENCP, ENSTORE_STATE, ERASE, EXPORT,
    FILE_FAMILY, FILE_FAMILY_WIDTH, FILE_FAMILY_WRAPPER, FILES, FILESIZE,
    FORCE,
    FORGET_ALL_IGNORED_STORAGE_GROUPS, FORGET_IGNORED_STORAGE_GROUP,
    GET_CRCS, GET_LAST_LOGFILE_NAME, GET_LOGFILE_NAME, GET_LOGFILES,
    GET_MAX_ENCP_LINES, GET_QUEUE, GET_REFRESH, GET_SUSPECT_VOLS,
    GET_UPDATE_INTERVAL, GET_WORK, GET_WORK_SORTED,
    HELP, HOST, HTML, HTML_DIR, HTML_FILE, HTML_GEN_HOST,
    ID, IGNORE_STORAGE_GROUP, IMPORT, IO,
    KEEP, KEEP_DIR,
    LAYER, LIBRARY, LIST, LOAD, LOG, LOGFILE_DIR, LS, LS_ACTIVE,
    MAX_ENCP_LINES, MAX_WORK, MESSAGE, MODIFY, MOUNT, 
    NAMEOF, NEW_LIBRARY, NO_ACCESS, NO_MAIL, NOTIFY, NOOUTAGE, NOOVERRIDE,
    OFFLINE, ONLINE, OPT, OUTAGE, OUTPUT_DIR, OVERRIDE,
    PARENT, PATH, PNFS_STATE, POSITION, PREFIX, PRIORITY,
    RAISE, READ_ONLY, RECURSIVE, RECYCLE, REFRESH, RESET_LIB, RESOLVE,
    RESTORE, RETRIES, RM, RM_ACTIVE_VOL, RM_SUSPECT_VOL, ROOT_ERROR,
    SAAG_STATUS, SENDTO, SET_CRCS, SEVERITY, SG, SHOW, SHOWID, SIZE,
    SHOW_IGNORED_STORAGE_GROUPS,
    START_DRAINING, START_TIME, STATUS, STOP_DRAINING, STOP_TIME,
    STORAGE_GROUP, SUBSCRIBE, SUMMARY,
    TAG, TAGECHO, TAGRM, TAGS, TEST, TIME, TIMEOUT, TITLE,
    UP, UPDATE, UPDATE_AND_EXIT, UPDATE_INTERVAL, USAGE,
    VOL, VOLS, VOLUME, VOL1OK,
    WARM_RESTART, WEB_HOST,
    XREF,
    ]

############################################################################

class Interface:

    def __init__(self, args=sys.argv, user_mode=0):
        if not user_mode: #Admin
            self.user_level = ADMIN
        else:
            self.user_level = USER

        self.argv = args
        self.options = {}
        self.help = 0
        self.usage = 0
        
        apply(self.compile_options_dict, self.valid_dictionaries())
        
        self.check_option_names()
        
        self.parse_options()

        self.config_host = self.default_host()
        self.config_port = self.default_port()

        if getattr(self, "help") and self.help:
            ret = self.print_help()
        if getattr(self, "usage") and self.usage:
            ret = self.print_usage()
        
############################################################################

    def default_host(self):
        val = os.environ.get('ENSTORE_CONFIG_HOST', DEFAULT_HOST)
        return val

    def default_port(self):
        val = os.environ.get('ENSTORE_CONFIG_PORT', DEFAULT_PORT)
        val = int(val)
        return val

############################################################################

    options = {}
    option_list = []
    args = []
    parameters = []
    
    alive_rcv_options = {
        TIMEOUT:{HELP_STRING:"number of seconds to wait for alive response",
                 VALUE_NAME:"alive_rcv_timeout",
                 VALUE_USAGE:REQUIRED,
                 VALUE_TYPE:INTEGER,
                 VALUE_LABEL:"seconds"},
        RETRIES:{HELP_STRING:"number of attempts to resend alive requests",
                 VALUE_NAME:"alive_retries",
                 VALUE_USAGE:REQUIRED,
                 VALUE_TYPE:INTEGER},
        }

    alive_options = alive_rcv_options.copy()
    alive_options[ALIVE] = {DEFAULT_VALUE:1,
                            HELP_STRING:
                            "prints message if the server is up or down.",
                            VALUE_TYPE:INTEGER,
                            VALUE_NAME:"alive",
                            VALUE_USAGE:IGNORED,
                            SHORT_OPTION:"a"
                            }
    help_options = {
        HELP:{DEFAULT_VALUE:1,
                HELP_STRING:"prints this messge",
                SHORT_OPTION:"h"},
        USAGE:{DEFAULT_VALUE:1,
                 VALUE_USAGE:IGNORED}
        }

    trace_options = {
        DO_PRINT:{VALUE_USAGE:REQUIRED,
                  VALUE_TYPE:RANGE,
                  HELP_STRING:"turns on more verbose output"},
        DONT_PRINT:{VALUE_USAGE:REQUIRED,
                    VALUE_TYPE:RANGE,
                    HELP_STRING:"turns off more verbose output"},
        DO_LOG:{VALUE_USAGE:REQUIRED,
                VALUE_TYPE:RANGE,
                HELP_STRING:"turns on more verbose logging"},
        DONT_LOG:{VALUE_USAGE:REQUIRED,
                  VALUE_TYPE:RANGE,
                  HELP_STRING:"turns off more verbose logging"},
        DO_ALARM:{VALUE_USAGE:REQUIRED,
                  VALUE_TYPE:RANGE,
                  HELP_STRING:"turns on more alarms"},
        DONT_ALARM:{VALUE_USAGE:REQUIRED,
                    VALUE_TYPE:RANGE,
                    HELP_STRING:"turns off more alarms"}
        }

    test_options = {
        'test':{DEFAULT_VALUE:2,
                DEFAULT_TYPE:INTEGER,
                HELP_STRING:"test",
                VALUE_USAGE:OPTIONAL,
                SHORT_OPTION:"t",
                USER_LEVEL:ADMIN
                },
        'opt':{HELP_STRING:"some string text",
               USER_LEVEL:USER,
               DEFAULT_NAME:'opt',
               DEFAULT_VALUE:DEFAULT,
               DEFAULT_TYPE:INTEGER,
               EXTRA_VALUES:[{VALUE_NAME:'filename',
                              VALUE_TYPE:STRING,
                              VALUE_USAGE:REQUIRED,
                              VALUE_LABEL:"filename",
                              },
                             {DEFAULT_NAME:"filename2",
                              DEFAULT_VALUE:"",
                              DEFAULT_TYPE:STRING,
                              VALUE_NAME:"filename2",
                              VALUE_TYPE:STRING,
                              VALUE_USAGE:OPTIONAL,
                              VALUE_LABEL:"filename2",
                              }]
               }
        }
                                     

############################################################################

    #lines_of_text: list of strings where each item in the list is a line of
    #               text that will be used to output the help string.
    #text_string: the string that will be appended to the end of lines_of_text
    #filler_length: 
    def build_help_string(self, lines_of_text, text_string,
                          filler_length, num_of_cols):
        #Build the non-help string part of the command output. Assume
        # that option_names is less than 80 characters.
        #lines_of_text = []
        try:
            last_line = lines_of_text[-1]
        except IndexError:
            last_line = ""

        if text_string:
            value_line_length = num_of_cols - len(last_line)
            index = 0
            while index < len(text_string):
                #calculate how much of the line can be used up without
                # splitting words on different lines.
                if (len(text_string) - index) < value_line_length:
                    new_index = len(text_string)
                else:
                    new_index = string.rfind(text_string, " ", index,
                                             index+value_line_length)
                #build each line (so far).
                if index == 0: #use existing line
                    try:
                        del lines_of_text[-1]
                    except IndexError:
                        pass

                    temp_fill = filler_length - len(last_line)
                    if temp_fill < 0:
                        temp_fill = 0
                    temp = ("%s" % (last_line,)) + " " * temp_fill + \
                                         text_string[index:new_index]
                    lines_of_text.append(temp)
                else: #use_new_line
                    lines_of_text.append(" " * filler_length +
                                        text_string[index:new_index].strip())
                index=new_index


    def print_help(self):
        #First print the usage line.
        print self.get_usage_line() + "\n"

        # num_of_cols - width of the terminal
        # COMM_COLS - length of option_names (aka "       --%-20s")
        num_of_cols = 80 #Assume this until python 2.1
        COMM_COLS = 29

        lines_of_text = [] #list of strings less than num_of_cols in length.
        
        list = self.options.keys()
        list.sort()
        for opts in list:

            #Don't even print out options that the user doesn't have access to.
            if self.options[opts].get(USER_LEVEL, USER) == ADMIN \
               and self.user_level == USER:
                continue

            #Snag all optional/required values that belong to this option.
            # Do this by getting the necessary fields from the dictionary.
            # (ie. "value_name"/"default_name" and "value_usage".)  Get the
            # list of extra options, if any.  Insert the values at the
            # beginning of the extras_args list.
            opt_arg = self.options[opts].get(
                VALUE_NAME,
                self.options[opts].get(DEFAULT_NAME, opts))
            opt_value = self.options[opts].get(VALUE_USAGE, IGNORED)
            opt_label = self.options[opts].get(VALUE_LABEL, opt_arg)
            extra_args = self.options[opts].get(EXTRA_VALUES, [])
            extra_args.insert(0, {VALUE_NAME:opt_arg,
                                  VALUE_USAGE:opt_value,
                                  VALUE_LABEL:opt_label})

            #Put together the string that specifies what the spelling of the
            # options are.  The two types are those with and without short
            # option equivalents.
            #ie: "   -a, --alive"
            if self.options[opts].get(SHORT_OPTION, None):
                #If option has a short argument equivalent.
                option_names = "   -%s, --%s" % \
                               (self.options[opts][SHORT_OPTION],
                                opts)
            else:
                #If option does not have a short argument equivalent.
                option_names = "       --%s" % (opts,)


            #Loop through the list generating the has_value string.  This
            # string is the list of values wrapped in [] or <> possible for
            # the option.
            #ie: "<VOLUME_NAME> <LIBRARY> <STORAGE_GROUP> <FILE_FAMILY>
            #     <WRAPPER> <MEDIA_TYPE> <VOLUME_BYTE_CAPACITY>"
            has_value = ""
            for opt_arg in extra_args:
                arg = string.upper(opt_arg.get(VALUE_LABEL,
                                               opt_arg.get(VALUE_NAME, BLANK)))
                arg = arg.replace("-", "_")
                value = opt_arg.get(VALUE_USAGE, IGNORED)
                                  
                if value == REQUIRED:
                    has_value = has_value + "<" + arg + "> "
                elif value == OPTIONAL:
                    has_value = has_value + "[" + arg + "] "

            #Get and calculate various variables needed to format the output.
            # help_string - shorter than accessing the dictionary
            help_string = self.options[opts].get(HELP_STRING, BLANK)

            lines_of_text = []
            #Build the OPTION part of the command output. Assume
            # that option_names is less than 80 characters.
            self.build_help_string(lines_of_text, option_names,
                                   0, num_of_cols)
            #For those options with values, insert the =.
            if has_value:
                self.build_help_string(lines_of_text, "=", 0, num_of_cols)
            #Build the VALUES part of the command output. Assume
            # that option_names is less than 80 characters.
            self.build_help_string(lines_of_text, has_value,
                                   0, num_of_cols)
            #Build the HELP STRING part of the command output. Assume
            # that option_names is less than 80 characters.
            self.build_help_string(lines_of_text, help_string,
                                   COMM_COLS, num_of_cols)
            
            for line in lines_of_text:
                print line
        sys.exit(0)

    def get_usage_line(self, opts=None): #The opts is legacy from interface.py.

        short_opts = self.getopt_short_options()
        if short_opts:
            short_opts = "-" + short_opts
        else:
            short_opts = ""

        usage_line = ""

        list = self.options.keys()
        list.sort()
        for key in list:

            #Deterimine if the option needs an "=" or "[=]" after it.
            has_value = self.options[key].get(VALUE_USAGE, IGNORED)
            if has_value == REQUIRED:
                has_value = "="
            elif has_value == OPTIONAL:
                has_value = "[=]"
            else:
                has_value = ""
                
            usage_line = usage_line + "--" + key + has_value + " "

        usage_string = "USAGE: " + sys.argv[0]
        if short_opts or usage_line:
            usage_string = usage_string + " [ " + short_opts + " " + \
                           usage_line + "] "
        usage_string = usage_string + self.format_parameters()
        return usage_string

    def format_parameters(self):
        param_string = ""
        for parameter in self.parameters:
            param_string = param_string + " " + parameter
        return param_string

    def print_usage(self, message=None):
        if message:
            print message

        print self.get_usage_line()
        sys.exit(0)

    def missing_parameter(self, param):
        sys.stderr.write("ERROR: missing parameter %s\n"%(param,))

############################################################################

    #This function returns the tuple containing the valid dictionaries used
    # in compile_options_dict().  Simply overload this function to
    # correctly set the valid option groups.
    def valid_dictionaries(self):
        return (self.help_options, self.test_options)

    #Compiles the dictionary groups into one massive dictionary named options.
    def compile_options_dict(self, *dictionaries):
        for i in range(0, len(dictionaries)):
            if type(dictionaries[i]) != type({}):
                raise TypeError, "Dictionary required, not %s." % \
                      type(dictionaries[i])
            for key in dictionaries[i].keys():
                if not self.options.has_key(key):
                    self.options[key] = dictionaries[i][key]

    #Verifies that the options used are in the list of options.  This is to
    # help cut down on the different combinations of spellings.
    def check_option_names(self):
        for opt in self.options.keys():
            if opt not in valid_option_list:
                msg = "Developer error.  Option '%s' not in valid option list."
                print msg % (opt,)
                sys.exit(1)

############################################################################

    #Goes through the compiled option dictionary looking for short options
    # to format in the getopt.getopt() format.
    def getopt_short_options(self):
        temp = ""
        for opt in self.options.keys():
            short_opt = self.options[opt].get(SHORT_OPTION, None)
            if short_opt and len(short_opt) == 1:
                temp = temp + short_opt
                
                if self.options[opt].get(VALUE_USAGE, None) in [REQUIRED]:
                    temp = temp + "="
                
        return temp

    #Goes through the compiled option dictionary pulling out long options
    # to format in the getopt.getopt() format.
    #The BC comment lines indicate backwards compatibility.  Some people
    # can't let go of VAX conventions.
    def getopt_long_options(self):
        temp = []
        for opt in self.options.keys():
            if self.options[opt].get(VALUE_USAGE, None) in [REQUIRED]:
                temp.append(opt + "=")
            else:
                temp.append(opt)
                
        return temp

############################################################################

    #Parse the command line.
    def parse_options(self):

        long_opts = self.getopt_long_options()
        short_opts = self.getopt_short_options()

        #If an argument uses an = for a value seperate it into two entries.
        self.split_on_equals(self.argv)
        argv = self.argv[1:]

        #For backward compatibility, convert options with underscores to
        # dashes.  This must be done before the getopt since the getopt breaks
        # with dashes.  It should be noted that the use of underscores is
        # a VAX thing, and that dashes is the UNIX way of things.
        self.convert_underscores(argv)

        #If the first thing is not an option (switch) place it with the
        # non-processeced arguments and remove it from the list of args.
        # This is done, because getopt.getopt() breaks if the first thing
        # it sees does not begin with a "-" or "--".
        while len(argv) and not self.is_option(argv[0]):
            self.args.append(argv[0])
            del argv[0]

        #There is a major problem with this method. Multiple entries on the
        # command line of the same command are not parsed properly.
        try:
            optlist, argv = getopt.getopt(argv, short_opts, long_opts)
        except getopt.GetoptError, detail:
            self.print_usage(detail.msg)

        #copy in this way, to keep self.args out of a dir() listing.
        for arg in argv:
            self.args.append(arg)

        for arg in optlist:
            opt = arg[0]
            value = arg[1]

            if self.is_admin_option(opt) and self.user_level == USER:
                #Deni access to admin commands if regular user.
                self.print_usage("option %s is an administrator option" %
                                 (opt,))

            if self.is_long_option(opt):
                #Option is a long option.  This means that the option is
                # preceded by two dashes and can be any length.
                self.long_option(opt[2:], value)

            elif self.is_short_option(opt):
                #Option is a short option.  This means it is only
                # one letter long and has one dash at the beginning
                # of the option group.
                self.short_option(opt[1:], value)

############################################################################

    #The option is a long option with value possible VALUE_USAGE.  Determine
    # if the option has been used in the correct manner.  If so, set the
    # value accordingly, if not print an error message.
    def long_option(self, long_opt, value):

        self.option_list.append(long_opt) #Rember this order.

        if self.options[long_opt].get(VALUE_USAGE, None) == REQUIRED:
            #First, determine if the option, which has been determined to
            # require a sub option, is followed in the command line with
            # an option that does not begin with "-" or "--".
            if value != None:
                self.set_value(long_opt, value)
            else:
                self.print_usage()
                
        elif self.options[long_opt].get(VALUE_USAGE, None) == OPTIONAL:
            next = self.next_argument(long_opt) #Used for optional.

            #First, determine if the option, which may or may not have a 
            # sub option, is followed in the command line with
            # an option that does not begin with "-" or "--".
            if value:
                self.set_value(long_opt, value)
                
            #If the option has an optional value and it is present then
            # find the value (albeit the hard way), set the value and then
            # remove the value from the list of previously unprocessed
            # arguments (self.args).
            elif next != None and not self.is_option(next):
                self.set_value(long_opt, next)
                self.args.remove(next)
            else:
                self.set_value(long_opt, None) #Uses 'default'
                
        else: #INGORED
            self.set_value(long_opt, None) #Uses 'default'

    #Do the same thing with the short options that is done with the long
    # options.  For all intensive purposes, this gets the long opt that
    # is the short option equivalet and uses the long opt.
    def short_option(self, short_opt, value):
        long_opt = self.short_to_long(short_opt)

        self.option_list.append(long_opt) #Rember this order.

        if self.options[long_opt].get(VALUE_USAGE, None) == REQUIRED:
            #First, determine if the option, which has been determined to
            # require a sub option, is followed in the command line with
            # an option that does not begin with "-" or "--".
            if value:
                self.set_value(long_opt, value)
            else:
                self.print_usage()

        elif self.options[long_opt].get(VALUE_USAGE, None) == OPTIONAL:
            #First, determine if the option, which may or may not have a 
            # sub option, is followed in the command line with
            # an option that does not begin with "-" or "--".
            if value:
                self.set_value(long_opt, value)
                
            #If the option has an optional value and it is present then
            # find the value (albeit the hard way), set the value and then
            # remove the value from the list of previously unprocessed
            # arguments (self.args).
            elif self.next_argument(short_opt) != None and \
                 not self.is_option(self.next_argument(short_opt)):
                next = self.next_argument(short_opt)
                self.set_value(long_opt, next)
                self.args.remove(next)
            else:
                self.set_value(long_opt, None) #Uses 'default'
                
        else: #INGORED
            self.set_value(long_opt, None) #Uses 'default'

############################################################################

    #Options can be entered like "--option value" or "--option=value".
    # Parse the argv passed in and split the "=" values into spaced value.
    def split_on_equals(self, argv):
        for i in range(len(argv)):
            #Make sure it is a switch.
            #Note: the replace operation is necessary to suport _s.
            if self.is_option(argv[i].split("=")[0].replace("_", "-")):
                list = string.split(argv[i], "=")
                argv[i:i + 1] = list

    def convert_underscores(self, argv):
        for i in range(0, len(argv)):
            if argv[i].find("_") >= 0: #returns -1 on failure
                opt_with_dashes = argv[i].replace("_", "-")
                if self.is_long_option(opt_with_dashes):
                    #sys.stderr.write("Option %s depreciated, " \
                    #                 "use %s instead.\n" %
                    #                 (argv[i], opt_with_dashes))
                    argv[i] = opt_with_dashes

    #This function is copied from the original interface code.  It parses,
    # a string into a list of integers (aka range).
    #Note: This should probably be looked at to be more robust.
    def parse_range(self, s):
        if ',' in s:
            s = string.split(s,',')
        else:
            s = [s]
        r = []
        for t in s:
            if '-' in t:
                lo, hi = string.split(t,'-')
                lo, hi = int(lo), int(hi)
                r.extend(range(lo, hi+1))
            else:
                r.append(int(t))
        return r

    #Take the passed in short option and return its long option equivalent.
    def short_to_long(self, short_opt):
        if not self.is_short_option(short_opt):
            return None
        for key in self.options.keys():
            if self.trim_short_option(short_opt) == \
               self.options[key].get(SHORT_OPTION, None):
                return key #in other words return the long opt.
        return None

    some_args = sys.argv[1:]
    #Return the next argument in the argument list after the one specified
    # as argument.  If one does not exist, return None.
    #some_args is used to avoid problems with duplicate arguments on the
    # command line.
    def next_argument(self, argument):
        #Get the next option after the option passed in.

        for arg in self.some_args:
            if string.find(arg, argument) != -1:
                index = self.some_args.index(arg)
                if index == len(self.some_args[1:]): #Nothing can follow.
                    return None
                rtn = self.some_args[index + 1]

                self.some_args = self.some_args[index + 1:]

                return rtn

        return None
            
############################################################################
    #These options remove leading "-" or "--" as appropriate from opt
    # and return.

    def trim_option(self, opt):
        if self.is_long_option(opt):
            return self.trim_long_option(opt)
        elif self.is_short_option(opt):
            return self.trim_short_option(opt)
        else:
            return opt
        
    def trim_long_option(self, opt):
        #There must be at least 3 characters.  Two from "--" and one
        # alphanumeric character.
        if len(opt) >= 3 and opt[:2] == "--" and (opt[2] in string.letters or
                                                  opt[2] in string.digits):
            return opt[2:]
        else:
            return opt
            
    def trim_short_option(self, opt):
        if len(opt) and opt[0] == "-" and (opt[1] in string.letters or
                                           opt[1] in string.digits):
            return opt[1:]
        else:
            return opt

############################################################################
    #These options return 1 if opt is the correct type of option,
    # otherwise it return 0.

    def is_option(self, opt):
        return self.is_long_option(opt) or self.is_short_option(opt)

    def is_long_option(self, opt):
        opt_check = self.trim_long_option(opt)
        try:
            for key in self.options.keys():
                opt_length = len(opt_check)
                #If the option (switch) matches in part return true.
                # Uniqueness will be tested by getopt.getopt().
                if len(key) >= opt_length and key[:opt_length] == opt_check:
                    return 1
            return 0
        except TypeError:
            return 0
    
    def is_short_option(self, opt):
        opt_check = self.trim_short_option(opt)
        try:
            for key in self.options.keys():
                if self.options[key].get(SHORT_OPTION, None) == opt_check:
                    return 1
            return 0
        except TypeError:
            return 0

############################################################################
    #Returns whether the option is a user or admin command.  A return
    # value of one means it is that type, 0 otherwise.

    def is_user_option(self, opt):
        if self.is_long_option(opt):
            if self.options[self.trim_option(opt)].get(
                USER_LEVEL, USER) == USER:
                return 1
        elif self.is_short_option(opt):
            long_opt = self.short_to_long(opt)
            if self.options[self.trim_option(long_opt)].get(
                USER_LEVEL, USER) == USER:
                return 1
        return 0
        
    def is_admin_option(self, opt):
        if self.is_long_option(opt):
            if self.options[self.trim_option(opt)].get(
                USER_LEVEL, USER) == ADMIN:
                return 1
        elif self.is_short_option(opt):
            long_opt = self.short_to_long(opt)
            if self.options[self.trim_option(long_opt)].get(
                USER_LEVEL, USER) == ADMIN:
                return 1
        return 0

############################################################################
    #These option return the correct value from the options dictionary
    # for a given long option long_opt and its dictionary opt_dict.  If
    # the dictionary doesn't have that particular value at hand, then
    # correctly determines the default value.

    def get_value_name(self, opt_dict, long_opt):
        # Determine what the variable's name is.  Use the command string
        # as the default if a "value_name" field is not specified.
        opt_name = opt_dict.get(VALUE_NAME, long_opt)
        
        #Convert command dashes to variable name underscores.
        opt_name = string.replace(opt_name, "-", "_") 
        
        return opt_name

    def get_default_name(self, opt_dict, long_opt):
        # Determine what the default's name is.  Use the command string
        # as the default if a "default_name" field is not specified.
        opt_name = opt_dict.get(DEFAULT_NAME, long_opt)
        
        #Convert command dashes to variable name underscores.
        opt_name = string.replace(opt_name, "-", "_") 
        
        return opt_name

    def get_default_value(self, opt_dict, value):
        #Return the DEFAULT_VALUE for an option that takes no value.
        if opt_dict.get(VALUE_USAGE, IGNORED) == IGNORED:
            return opt_dict.get(DEFAULT_VALUE, DEFAULT)
        #Return the DEFAULT_VALUE for an option that takes an optional value.
        elif opt_dict.get(VALUE_USAGE, IGNORED) == OPTIONAL:
            if value == None and opt_dict.get(DEFAULT_VALUE, None):
                return opt_dict[DEFAULT_VALUE]
            elif value == None and opt_dict.get(FORCE_SET_DEFAULT, None):
                return opt_dict.get(DEFAULT_VALUE, DEFAULT)
            else:
                return value
          #Return the DEFAULT_VALUE for an option that must take a value.
        # Usually this will be set to the value passed in, unless
        # FORCE_SET_DEFAULT forces the setting of both values.
        else: #REQUIRED
            if opt_dict.get(FORCE_SET_DEFAULT, None):
                return opt_dict.get(DEFAULT_VALUE, DEFAULT)
            else:
                return value
    
    def get_value_type(self, opt_dict, value):
        if opt_dict.get(VALUE_TYPE, STRING) == INTEGER:
            return int(value)
        elif opt_dict.get(VALUE_TYPE, STRING) == FLOAT:
            return float(value)
        elif opt_dict.get(VALUE_TYPE, STRING) == RANGE:
            return self.parse_range(value)
        else: #STRING
            return str(value)

    def get_default_type(self, opt_dict, value):
        if opt_dict.get(DEFAULT_TYPE, STRING) == INTEGER:
            return int(value)
        elif opt_dict.get(DEFAULT_TYPE, STRING) == FLOAT:
            return float(value)
        elif opt_dict.get(DEFAULT_TYPE, STRING) == RANGE:
            return self.parse_range(value)
        else: #STRING
            return str(value)

############################################################################
    #Thse options set the values in the interface class.  set_value() is
    # the function that calls the others.  set_from_dcitionary() takes the
    # specific dictionary (which is important when multiple arguments for
    # a single option exist) and sets the interface variables.  The last
    # function, set_extra_values(), handles when more than one argument
    # is parsed for an option.
       
    def set_value(self, long_opt, value):
        #Make sure the name gets put inside if it isn't there already.
        if not self.options[long_opt].get(VALUE_NAME, None):
            self.options[long_opt][VALUE_NAME] = long_opt

        #Pass in the long option dictionary, long option and its value to
        # use them to set this interfaces variable.
        self.set_from_dictionary(self.options[long_opt], long_opt, value)

        #Some options may require more than one value.
        self.set_extra_values(long_opt, value)

    def set_from_dictionary(self, opt_dict, long_opt, value):
        if value:
            #Get the name to set.
            opt_name = self.get_value_name(opt_dict, long_opt)

            #Get the value in the correct type to set.
            try:
                #opt_value = self.get_value_value(opt_dict, value)
                opt_typed_value = self.get_value_type(opt_dict, value)
            except ValueError, detail:
                msg = "option %s requires type %s" % \
                      (long_opt,
                       opt_dict.get(VALUE_TYPE, STRING))
                self.print_usage(msg)

            setattr(self, opt_name, opt_typed_value)
            
        if not value or opt_dict.get(FORCE_SET_DEFAULT, None):
            #Get the name to set.
            opt_name = self.get_default_name(opt_dict, long_opt)

            #Get the value in the correct type to set.
            try:
                opt_value = self.get_default_value(opt_dict, value)
                opt_typed_value = self.get_default_type(opt_dict, opt_value)
            except ValueError, detail:
                msg = "option %s requires type %s" % \
                      (long_opt,
                       opt_dict.get(DEFAULT_TYPE, STRING))
                self.print_usage(msg)

            setattr(self, opt_name, opt_typed_value)

    def set_extra_values(self, opt, value):
        if self.is_short_option(opt):
            long_opt = self.short_to_long(opt)
        else:
               long_opt = opt

        extras = self.options[self.trim_option(long_opt)].get(
            EXTRA_VALUES, [])

        next = None
        
        for extra_option in extras:
            if next:
                next = self.next_argument(next)
            elif value:
                next = self.next_argument(value)
            else:
                next = self.next_argument(opt)

            self.set_from_dictionary(extra_option, long_opt, next)
            try:
                if next:
                    self.args.remove(next)
            except ValueError:
                sys.stderr.write("Problem processing argument %s." % (next,))

############################################################################
    
if __name__ == '__main__':
    intf = Interface()

    #print the options value
    for arg in dir(intf):
        if string.replace(arg, "_", "-") in intf.options.keys():
            print arg, type(getattr(intf, arg)), ": ",
            pprint.pprint(getattr(intf, arg))

    print

    #every other matched value
    for arg in dir(intf):
        if string.replace(arg, "_", "-") not in intf.options.keys():
            print arg, type(getattr(intf, arg)), ": ",
            pprint.pprint(getattr(intf, arg))

    print

    if intf.args:
        print "unprocessed args:", intf.args

    if getattr(intf, "help", None):
        intf.print_help()
    if getattr(intf, "usage", None):
        intf.print_usage()

